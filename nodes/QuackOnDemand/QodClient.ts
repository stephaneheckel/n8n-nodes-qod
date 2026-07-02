// FlightSQL client for Quack-on-Demand, packaged for an n8n node (CommonJS).
//
// Node has no first-party Flight SQL driver, so this talks to the edge over raw
// gRPC (@grpc/grpc-js, a pure-JS gRPC stack with no native addon, which is what
// makes it safe to bundle in an n8n community node) and decodes the Arrow
// result stream with apache-arrow.
//
// Unlike the standalone example, the Flight + Flight SQL protocol is inlined as
// proto source and loaded with protobuf.parse + protoLoader.fromJSON, so there
// is no .proto asset to ship in the published package.
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as protobuf from 'protobufjs';
import * as tls from 'node:tls';
import { tableFromIPC, tableFromJSON, tableToIPC, Table } from 'apache-arrow';

const FLIGHTSQL_NAMESPACE = 'type.googleapis.com/arrow.flight.protocol.sql';

// Minimal slice of Apache Arrow Flight (Flight.proto): only the messages and
// RPCs this client uses.
const FLIGHT_PROTO = `
syntax = "proto3";
package arrow.flight.protocol;

service FlightService {
  rpc Handshake(HandshakeRequest) returns (HandshakeResponse) {}
  rpc GetFlightInfo(FlightDescriptor) returns (FlightInfo) {}
  rpc DoGet(Ticket) returns (stream FlightData) {}
  rpc DoPut(stream FlightData) returns (stream PutResult) {}
}

message FlightDescriptor {
  enum DescriptorType { UNKNOWN = 0; PATH = 1; CMD = 2; }
  DescriptorType type = 1;
  bytes cmd = 2;
  repeated string path = 3;
}

message Ticket { bytes ticket = 1; }
message Location { string uri = 1; }

message FlightEndpoint {
  Ticket ticket = 1;
  repeated Location location = 2;
}

message FlightInfo {
  bytes schema = 1;
  FlightDescriptor flight_descriptor = 2;
  repeated FlightEndpoint endpoint = 3;
  int64 total_records = 4;
  int64 total_bytes = 5;
  bool ordered = 6;
  bytes app_metadata = 7;
}

message FlightData {
  FlightDescriptor flight_descriptor = 1;
  bytes data_header = 2;
  bytes app_metadata = 3;
  bytes data_body = 1000;
}

message PutResult {
  bytes app_metadata = 1;
}

message HandshakeRequest {
  bytes payload = 1;
  uint64 protocol_version = 2;
}

message HandshakeResponse {
  bytes payload = 1;
  uint64 protocol_version = 2;
}
`;

// Flight SQL (FlightSql.proto) messages. Any is declared as a plain message
// because its wire format matches google.protobuf.Any.  Added catalog browsing
// and DML commands so the n8n node can offer a zero-SQL table browser.
const FLIGHTSQL_PROTO = `
syntax = "proto3";
package arrow.flight.protocol.sql;

message Any {
  string type_url = 1;
  bytes value = 2;
}

// --- query / update ---

message CommandStatementQuery {
  string query = 1;
  bytes transaction_id = 2;
}

message CommandStatementUpdate {
  string query = 1;
  bytes transaction_id = 2;
}

// --- bulk ingestion ---

message CommandStatementIngest {
  bytes transaction_id = 1;
}

// --- catalog browsing ---

message CommandGetCatalogs {}

message CommandGetDbSchemas {
  string catalog = 1;
  string db_schema_filter_pattern = 2;
}

message CommandGetTables {
  string catalog = 1;
  string db_schema_filter_pattern = 2;
  string table_name_filter_pattern = 3;
  repeated string table_types = 4;
  bool include_schema = 5;
}`;

export interface QodConfig {
	host: string;
	port: number;
	user: string;
	password: string;
	tenant: string;
	pool: string;
	superuser: boolean;
	tls: boolean;
	tlsVerify: boolean;
}

export interface CatalogRow {
	name: string;
	type?: string;
	schema?: string;
	dataType?: string;
	nullable?: boolean;
}

interface FlightClient extends grpc.Client {
	Handshake(
		req: unknown,
		metadata: grpc.Metadata,
		cb: (err: grpc.ServiceError | null, resp: any) => void,
	): void;
	GetFlightInfo(
		descriptor: unknown,
		metadata: grpc.Metadata,
		cb: (err: grpc.ServiceError | null, info: any) => void,
	): void;
	DoGet(ticket: unknown, metadata: grpc.Metadata): grpc.ClientReadableStream<any>;
	DoPut(metadata: grpc.Metadata): grpc.ClientDuplexStream<any, any>;
}

function typeUrl(name: string): string {
	return `${FLIGHTSQL_NAMESPACE}.${name}`;
}

// Pull the server's self-signed leaf certificate off the wire and return it as
// PEM, so it can be pinned as the gRPC root. Combined with a no-op
// checkServerIdentity this is the equivalent of "skip verification".
function fetchServerCertPem(host: string, port: number): Promise<string> {
	const isIp = /^[\d.]+$/.test(host) || host.includes(':');
	return new Promise((resolve, reject) => {
		const socket = tls.connect(
			{ host, port, rejectUnauthorized: false, ...(isIp ? {} : { servername: host }) },
			() => {
				const cert = socket.getPeerCertificate(true);
				socket.end();
				if (!cert || !cert.raw) {
					reject(new Error('server presented no certificate'));
					return;
				}
				const b64 = cert.raw.toString('base64').match(/.{1,64}/g)!.join('\n');
				resolve(`-----BEGIN CERTIFICATE-----\n${b64}\n-----END CERTIFICATE-----\n`);
			},
		);
		socket.on('error', reject);
	});
}

// Reassemble the encapsulated Arrow IPC message format the stream reader expects
// from a FlightData chunk (header + body):
//   [continuation 0xFFFFFFFF][int32 LE header length, padded to 8][header][body]
function encapsulate(header: Buffer, body: Buffer): Buffer {
	const padded = (header.length + 7) & ~7;
	const prefix = Buffer.alloc(8 + padded);
	prefix.writeUInt32LE(0xffffffff, 0);
	prefix.writeInt32LE(padded, 4);
	header.copy(prefix, 8);
	return Buffer.concat([prefix, body]);
}

const EOS = Buffer.from([0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00]);

// Convert one Arrow row into a plain JSON object suitable for an n8n item.
// Int64 columns come back as BigInt and Decimal columns as DecimalBigNum
// objects, neither of which serializes cleanly, so both are coerced to strings.
function rowToObject(row: any, table: Table): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	for (const field of table.schema.fields) {
		const value = row[field.name];
		out[field.name] =
			typeof value === 'bigint'
				? value.toString()
				: value != null && typeof value === 'object'
					? String(value)
					: value;
	}
	return out;
}

export class QodClient {
	private readonly commandQueryType: protobuf.Type;
	private readonly commandUpdateType: protobuf.Type;
	private readonly getCatalogsType: protobuf.Type;
	private readonly getDbSchemasType: protobuf.Type;
	private readonly getTablesType: protobuf.Type;
	private readonly commandIngestType: protobuf.Type;

	// Bearer token from Flight handshake (GizmoSQL-style auth).
	// Undefined = use HTTP Basic (QoD, sqlflite).
	private readonly bearerToken?: string;

	private constructor(
		private readonly client: FlightClient,
		private readonly cfg: QodConfig,
		private readonly anyType: protobuf.Type,
		cmdTypes: {
			commandQuery: protobuf.Type;
			commandUpdate: protobuf.Type;
			commandIngest: protobuf.Type;
			getCatalogs: protobuf.Type;
			getDbSchemas: protobuf.Type;
			getTables: protobuf.Type;
		},
		bearerToken?: string,
	) {
		this.bearerToken = bearerToken;
		this.commandQueryType = cmdTypes.commandQuery;
		this.commandUpdateType = cmdTypes.commandUpdate;
		this.commandIngestType = cmdTypes.commandIngest;
		this.getCatalogsType = cmdTypes.getCatalogs;
		this.getDbSchemasType = cmdTypes.getDbSchemas;
		this.getTablesType = cmdTypes.getTables;
	}

	static async connect(cfg: QodConfig): Promise<QodClient> {
		const creds = await QodClient.credentials(cfg);

		const root = new protobuf.Root();
		protobuf.parse(FLIGHT_PROTO, root);
		protobuf.parse(FLIGHTSQL_PROTO, root);

		const pkgDef = protoLoader.fromJSON(root.toJSON(), {
			keepCase: true,
			longs: String,
			enums: String,
			defaults: true,
			oneofs: true,
		});
		const proto = grpc.loadPackageDefinition(pkgDef) as any;
		const FlightService = proto.arrow.flight.protocol.FlightService;

		// grpc-js refuses an IP literal as the TLS server name. When skipping
		// verification anyway, override the SSL target name with a placeholder.
		const options: grpc.ClientOptions = {};
		if (cfg.tls && !cfg.tlsVerify) {
			options['grpc.ssl_target_name_override'] = 'quack-on-demand';
			options['grpc.default_authority'] = 'quack-on-demand';
		}
		const client: FlightClient = new FlightService(`${cfg.host}:${cfg.port}`, creds, options);

		const anyType = root.lookupType('arrow.flight.protocol.sql.Any');
		const cmdTypes = {
			commandQuery: root.lookupType('arrow.flight.protocol.sql.CommandStatementQuery'),
			commandUpdate: root.lookupType('arrow.flight.protocol.sql.CommandStatementUpdate'),
			commandIngest: root.lookupType('arrow.flight.protocol.sql.CommandStatementIngest'),
			getCatalogs: root.lookupType('arrow.flight.protocol.sql.CommandGetCatalogs'),
			getDbSchemas: root.lookupType('arrow.flight.protocol.sql.CommandGetDbSchemas'),
			getTables: root.lookupType('arrow.flight.protocol.sql.CommandGetTables'),
		};

		// Try Flight handshake for Bearer token auth (GizmoSQL).
		// Fall back to HTTP Basic silently if the server doesn't implement Handshake.
		let bearerToken: string | undefined;
		if (cfg.user || cfg.password) {
			try {
				const basic = Buffer.from(`${cfg.user}:${cfg.password}`).toString('base64');
				const md = new grpc.Metadata();
				md.set('authorization', `Basic ${basic}`);
				// GizmoSQL-style: Basic auth header + username:password in payload
				const payload = Buffer.from(`${cfg.user}:${cfg.password}`);

				const resp: any = await new Promise((resolve, reject) => {
					client.Handshake(
						{ payload, protocolVersion: 0 },
						md,
						(err, r) => (err ? reject(err) : resolve(r)),
					);
				});
				const rx = root.lookupType('arrow.flight.protocol.HandshakeResponse');
				const decoded = rx.decode(resp.payload || Buffer.alloc(0));
				bearerToken = Buffer.from((decoded as any).payload || '').toString('utf8');
			} catch {
				// Server doesn't support Handshake — use HTTP Basic below
			}
		}

		return new QodClient(client, cfg, anyType, cmdTypes, bearerToken);
	}

	private static async credentials(cfg: QodConfig): Promise<grpc.ChannelCredentials> {
		if (!cfg.tls) return grpc.credentials.createInsecure();
		if (cfg.tlsVerify) return grpc.credentials.createSsl();
		const pem = await fetchServerCertPem(cfg.host, cfg.port);
		return grpc.credentials.createSsl(Buffer.from(pem), null, null, {
			checkServerIdentity: () => undefined,
		});
	}

	// The edge reads these headers on every RPC. authorization is HTTP Basic;
	// tenant/pool route the query; superuser=true selects the system realm and
	// bypasses the per-statement ACL gate.
	private metadata(): grpc.Metadata {
		const md = new grpc.Metadata();
		md.set('tenant', this.cfg.tenant);
		md.set('pool', this.cfg.pool);
		if (this.bearerToken) {
			md.set('authorization', `Bearer ${this.bearerToken}`);
		} else if (this.cfg.user || this.cfg.password) {
			const basic = Buffer.from(`${this.cfg.user}:${this.cfg.password}`).toString('base64');
			md.set('authorization', `Basic ${basic}`);
		}
		if (this.cfg.superuser) md.set('superuser', 'true');
		return md;
	}

	// Build the FlightDescriptor.cmd: an Any-wrapped protobuf message.
	private buildCommand(messageType: protobuf.Type, payload: Record<string, unknown>, commandName: string): Buffer {
		const inner = messageType.encode(payload).finish();
		const any = this.anyType.encode({ typeUrl: typeUrl(commandName), value: inner }).finish();
		return Buffer.from(any);
	}

	// Core RPC: encode a command, call GetFlightInfo + DoGet, decode the Arrow
	// stream, and return the raw Arrow Table (with proper Buffer columns).
	private async executeCommandRaw(
		messageType: protobuf.Type,
		payload: Record<string, unknown>,
		commandName: string,
	): Promise<Table> {
		const cmd = this.buildCommand(messageType, payload, commandName);

		const info = await new Promise<any>((resolve, reject) => {
			this.client.GetFlightInfo(
				{ type: 'CMD', cmd },
				this.metadata(),
				(err, resp) => (err ? reject(err) : resolve(resp)),
			);
		});

		const messages: Buffer[] = [];
		const endpoints = info.endpoint || [];
		for (const endpoint of endpoints) {
			await new Promise<void>((resolve, reject) => {
				const stream = this.client.DoGet({ ticket: endpoint.ticket.ticket }, this.metadata());
				stream.on('data', (fd: any) => {
					const header: Buffer = fd.dataHeader || fd.data_header || Buffer.alloc(0);
					const body: Buffer = fd.dataBody || fd.data_body || Buffer.alloc(0);
					if (header.length > 0) messages.push(encapsulate(header, body));
				});
				stream.on('end', resolve);
				stream.on('error', reject);
			});
		}

		return tableFromIPC(Buffer.concat([...messages, EOS]));
	}

	// Core RPC: same as above but returns plain-object rows (via rowToObject).
	private async executeCommand(
		messageType: protobuf.Type,
		payload: Record<string, unknown>,
		commandName: string,
	): Promise<Array<Record<string, unknown>>> {
		const table = await this.executeCommandRaw(messageType, payload, commandName);
		return table.toArray().map((row) => rowToObject(row, table));
	}

	// ── Public API ────────────────────────────────────────────────────────

	// Run one SQL statement and return the rows as plain objects.
	async query(sql: string): Promise<Array<Record<string, unknown>>> {
		return this.executeCommand(this.commandQueryType, { query: sql }, 'CommandStatementQuery');
	}

	// Execute a DML / DDL statement (INSERT, UPDATE, DELETE, CREATE, etc.).
	// Returns the Arrow stream which is typically empty for DML, but some
	// statements (e.g. INSERT … RETURNING) may produce rows.
	async update(sql: string): Promise<Array<Record<string, unknown>>> {
		return this.executeCommand(this.commandUpdateType, { query: sql }, 'CommandStatementUpdate');
	}

	// Bulk insert: convert JSON rows to Arrow, stream via DoPut.
	// Faster than multi-VALUES INSERT for large datasets (>1K rows).
	async ingest(sql: string, rows: Array<Record<string, unknown>>): Promise<void> {
		if (rows.length === 0) return;

		// Build Arrow table from the rows
		const arrowTable = tableFromJSON(rows);

		// Serialize to Arrow IPC stream format (includes schema + record batches)
		const ipcBuffer = Buffer.from(tableToIPC(arrowTable, 'stream'));

		// Build CommandStatementQuery with the INSERT SQL (QoD only supports Query)
		const cmd = this.buildCommand(this.commandQueryType, { query: sql }, 'CommandStatementQuery');

		const info = await new Promise<any>((resolve, reject) => {
			this.client.GetFlightInfo(
				{ type: 'CMD', cmd },
				this.metadata(),
				(err, resp) => (err ? reject(err) : resolve(resp)),
			);
		});

		// DoPut to each endpoint — first message carries the descriptor,
		// then the Arrow IPC data
		for (const endpoint of info.endpoint || []) {
			await new Promise<void>((resolve, reject) => {
				const stream = this.client.DoPut(this.metadata());
				stream.on('error', reject);
				stream.on('end', resolve);
				// Write the descriptor first
				stream.write({ flight_descriptor: { type: 'CMD', cmd }, app_metadata: endpoint.ticket.ticket });
				// Then the Arrow IPC stream
				stream.write({ data_body: ipcBuffer });
				stream.end();
			});
		}
	}

	// ── Catalog operations ───────────────────────────────────────────────
	//
	// QoD implements the standard FlightSQL catalog protocol.
	// getColumns uses CommandGetTables with include_schema=true, parsing the
	// Arrow Schema IPC blob — this is database-agnostic (no SQL).

	async getCatalogs(): Promise<string[]> {
		const rows = await this.executeCommand(this.getCatalogsType, {}, 'CommandGetCatalogs');
		return rows.map((r) => String(r.catalog_name || ''));
	}

	async getSchemas(catalog: string): Promise<string[]> {
		const rows = await this.executeCommand(
			this.getDbSchemasType,
			{ catalog },
			'CommandGetDbSchemas',
		);
		return rows.map((r) => String(r.db_schema_name || ''));
	}

	async getTables(catalog: string, schema: string): Promise<CatalogRow[]> {
		const rows = await this.executeCommand(
			this.getTablesType,
			{ catalog, dbSchemaFilterPattern: schema },
			'CommandGetTables',
		);
		return rows.map((r) => ({
			name: String(r.table_name || ''),
			type: String(r.table_type || 'TABLE'),
		}));
	}

	async getColumns(catalog: string, schema: string, table: string): Promise<CatalogRow[]> {
		// Use CommandGetTables with include_schema=true and filter to the
		// specific table.  The table_schema column is an Arrow Schema IPC blob
		// — we use executeCommandRaw to get the raw Arrow Table so Buffer
		// columns are preserved (rowToObject corrupts them to "[object Buffer]").
		const arrowTable = await this.executeCommandRaw(
			this.getTablesType,
			{ catalog, dbSchemaFilterPattern: schema, tableNameFilterPattern: table, includeSchema: true },
			'CommandGetTables',
		);

		// Find the row for our table by scanning the Arrow table column.
		const nameCol = arrowTable.getChild('table_name');
		if (!nameCol) return [];

		let targetIdx = -1;
		for (let i = 0; i < arrowTable.numRows; i++) {
			if (String(nameCol.get(i)) === table) {
				targetIdx = i;
				break;
			}
		}
		if (targetIdx < 0) return [];

		const schemaCol = arrowTable.getChild('table_schema');
		if (!schemaCol) return [];
		const schemaBytes = schemaCol.get(targetIdx) as Buffer | null;
		if (!schemaBytes || schemaBytes.length === 0) return [];

		// Parse the Arrow Schema IPC blob.
		const schemaTable = tableFromIPC(schemaBytes);
		return schemaTable.schema.fields.map((field) => ({
			name: field.name,
			dataType: String(field.type),
			nullable: field.nullable,
		}));
	}

	close(): void {
		this.client.close();
	}
}

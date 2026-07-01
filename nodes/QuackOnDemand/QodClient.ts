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
import { tableFromIPC, Table } from 'apache-arrow';

const FLIGHTSQL_NAMESPACE = 'type.googleapis.com/arrow.flight.protocol.sql';

// Minimal slice of Apache Arrow Flight (Flight.proto): only the messages and
// RPCs this client uses.
const FLIGHT_PROTO = `
syntax = "proto3";
package arrow.flight.protocol;

service FlightService {
  rpc GetFlightInfo(FlightDescriptor) returns (FlightInfo) {}
  rpc DoGet(Ticket) returns (stream FlightData) {}
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

// --- catalog browsing ---

message CommandGetCatalogs {}

message CommandGetSchemas {
  string catalog = 1;
  string db_schema_filter_pattern = 2;
}

message CommandGetTables {
  string catalog = 1;
  string db_schema_filter_pattern = 2;
  string table_name_filter_pattern = 3;
  repeated string table_types = 4;
  bool include_schema = 5;
}

message CommandGetColumns {
  string catalog = 1;
  string db_schema_filter_pattern = 2;
  string table_name_filter_pattern = 3;
  string column_name_filter_pattern = 4;
}
`;

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
	GetFlightInfo(
		descriptor: unknown,
		metadata: grpc.Metadata,
		cb: (err: grpc.ServiceError | null, info: any) => void,
	): void;
	DoGet(ticket: unknown, metadata: grpc.Metadata): grpc.ClientReadableStream<any>;
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
	private readonly getSchemasType: protobuf.Type;
	private readonly getTablesType: protobuf.Type;
	private readonly getColumnsType: protobuf.Type;

	private constructor(
		private readonly client: FlightClient,
		private readonly cfg: QodConfig,
		private readonly anyType: protobuf.Type,
		cmdTypes: {
			commandQuery: protobuf.Type;
			commandUpdate: protobuf.Type;
			getCatalogs: protobuf.Type;
			getSchemas: protobuf.Type;
			getTables: protobuf.Type;
			getColumns: protobuf.Type;
		},
	) {
		this.commandQueryType = cmdTypes.commandQuery;
		this.commandUpdateType = cmdTypes.commandUpdate;
		this.getCatalogsType = cmdTypes.getCatalogs;
		this.getSchemasType = cmdTypes.getSchemas;
		this.getTablesType = cmdTypes.getTables;
		this.getColumnsType = cmdTypes.getColumns;
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
			getCatalogs: root.lookupType('arrow.flight.protocol.sql.CommandGetCatalogs'),
			getSchemas: root.lookupType('arrow.flight.protocol.sql.CommandGetSchemas'),
			getTables: root.lookupType('arrow.flight.protocol.sql.CommandGetTables'),
			getColumns: root.lookupType('arrow.flight.protocol.sql.CommandGetColumns'),
		};
		return new QodClient(client, cfg, anyType, cmdTypes);
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
		const basic = Buffer.from(`${this.cfg.user}:${this.cfg.password}`).toString('base64');
		md.set('authorization', `Basic ${basic}`);
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
	// stream, and return the rows as plain objects.
	private async executeCommand(
		messageType: protobuf.Type,
		payload: Record<string, unknown>,
		commandName: string,
	): Promise<Array<Record<string, unknown>>> {
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

		const table = tableFromIPC(Buffer.concat([...messages, EOS]));
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

	// ── Catalog operations ───────────────────────────────────────────────

	async getCatalogs(): Promise<string[]> {
		const rows = await this.executeCommand(this.getCatalogsType, {}, 'CommandGetCatalogs');
		return rows.map((r) => String(r.catalog_name || ''));
	}

	async getSchemas(catalog: string): Promise<string[]> {
		const rows = await this.executeCommand(
			this.getSchemasType,
			{ catalog, dbSchemaFilterPattern: '' },
			'CommandGetSchemas',
		);
		return rows.map((r) => String(r.db_schema_name || ''));
	}

	async getTables(catalog: string, schema: string): Promise<CatalogRow[]> {
		const rows = await this.executeCommand(
			this.getTablesType,
			{ catalog, dbSchemaFilterPattern: schema, tableNameFilterPattern: '', tableTypes: [], includeSchema: false },
			'CommandGetTables',
		);
		return rows.map((r) => ({
			name: String(r.table_name || ''),
			type: String(r.table_type || 'TABLE'),
		}));
	}

	async getColumns(catalog: string, schema: string, table: string): Promise<CatalogRow[]> {
		const rows = await this.executeCommand(
			this.getColumnsType,
			{
				catalog,
				dbSchemaFilterPattern: schema,
				tableNameFilterPattern: table,
				columnNameFilterPattern: '',
			},
			'CommandGetColumns',
		);
		return rows.map((r) => ({
			name: String(r.column_name || ''),
			dataType: String(r.data_type || ''),
			nullable: r.is_nullable !== undefined ? Boolean(r.is_nullable) : true,
		}));
	}

	close(): void {
		this.client.close();
	}
}

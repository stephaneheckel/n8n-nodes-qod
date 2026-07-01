import {
	IDataObject,
	IExecuteFunctions,
	ILoadOptionsFunctions,
	INodeExecutionData,
	INodePropertyOptions,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';

import { QodClient, QodConfig } from './QodClient';

function credentialsToConfig(c: Record<string, unknown>): QodConfig {
	return {
		host: c.host as string,
		port: c.port as number,
		user: c.user as string,
		password: c.password as string,
		tenant: c.tenant as string,
		pool: c.pool as string,
		superuser: c.superuser as boolean,
		tls: c.tls as boolean,
		tlsVerify: c.tlsVerify as boolean,
	};
}

// Shared helper: open a connection, run a catalog call, return formatted
// options. Used by all loadOptions methods.
async function withClient<T>(
	ctx: ILoadOptionsFunctions,
	fn: (client: QodClient) => Promise<T>,
): Promise<T> {
	const creds = await ctx.getCredentials('quackOnDemandApi');
	const cfg = credentialsToConfig(creds);
	const client = await QodClient.connect(cfg);
	try {
		return await fn(client);
	} finally {
		client.close();
	}
}

// ── Resource handlers (free functions — execute() has 'this: IExecuteFunctions') ──

async function handleQuery(
	exec: IExecuteFunctions,
	client: QodClient,
	itemIndex: number,
): Promise<Array<Record<string, unknown>>> {
	const operation = exec.getNodeParameter('operation', itemIndex) as string;
	const sql = exec.getNodeParameter('query', itemIndex) as string;

	if (operation === 'executeUpdate') {
		const rows = await client.query(sql);  // DDL/DML via query() — DuckDB compatible
		if (rows.length === 0) return [{ success: true, message: 'Statement executed successfully' }];
		return rows;
	}
	const rows = await client.query(sql);
	if (rows.length === 0) return [{ success: true, message: 'Query executed — no rows returned' }];
	return rows;
}

async function handleTable(
	exec: IExecuteFunctions,
	client: QodClient,
	itemIndex: number,
): Promise<Array<Record<string, unknown>>> {
	const operation = exec.getNodeParameter('operation', itemIndex) as string;
	const schema = exec.getNodeParameter('schema', itemIndex) as string;
	const tableName = exec.getNodeParameter('table', itemIndex) as string;

	if (operation === 'read') {
		const columns = exec.getNodeParameter('columns', itemIndex, []) as string[];
		const filter = exec.getNodeParameter('filter', itemIndex, '') as string;
		const limit = exec.getNodeParameter('limit', itemIndex, 100) as number;

		const cols = columns && columns.length > 0 ? columns.join(', ') : '*';
		let sql = `SELECT ${cols} FROM ${schema}.${tableName}`;
		if (filter && filter.trim()) {
			sql += ` WHERE ${filter.trim()}`;
		}
		if (limit > 0) {
			sql += ` LIMIT ${limit}`;
		}

		return client.query(sql);
	}

	if (operation === 'insert') {
		const items = exec.getInputData();
		const inputJson = items[itemIndex]?.json || {};
		const valuesJson = exec.getNodeParameter('valuesJson', itemIndex, {}) as Record<string, unknown>;
		// Merge: valuesJson takes precedence over input JSON keys
		const data: Record<string, unknown> = { ...inputJson, ...(valuesJson && Object.keys(valuesJson).length > 0 ? valuesJson : {}) };
		if (Object.keys(data).length === 0) {
			throw new NodeOperationError(exec.getNode(), 'No data to insert. Provide Values (JSON) or pass data from an upstream node.', { itemIndex });
		}

		const selectedColumns = exec.getNodeParameter('columns', itemIndex, []) as string[];
		const cols = selectedColumns.length > 0 ? selectedColumns : Object.keys(data);

		const vals = cols.map((col) => {
			const val = data[col];
			if (val === undefined || val === null) return 'NULL';
			if (typeof val === 'number' || typeof val === 'boolean') return String(val);
			return `'${String(val).replace(/'/g, "''")}'`;
		});

		const sql = `INSERT INTO ${schema}.${tableName} (${cols.join(', ')}) VALUES (${vals.join(', ')})`;
		await client.query(sql);  // DuckDB handles DML via query() — safer than update()
		return [{ query: sql, rows_affected: 1 }];
	}

	if (operation === 'update') {
		const items = exec.getInputData();
		const inputJson = items[itemIndex]?.json || {};
		const valuesJson = exec.getNodeParameter('valuesJson', itemIndex, {}) as Record<string, unknown>;
		const data: Record<string, unknown> = { ...inputJson, ...(valuesJson && Object.keys(valuesJson).length > 0 ? valuesJson : {}) };
		if (Object.keys(data).length === 0) {
			throw new NodeOperationError(exec.getNode(), 'No data to update. Provide Values (JSON) or pass data from an upstream node.', { itemIndex });
		}

		const selectedColumns = exec.getNodeParameter('columns', itemIndex, []) as string[];
		const filter = exec.getNodeParameter('filter', itemIndex, '') as string;
		if (!filter || !filter.trim()) {
			throw new NodeOperationError(exec.getNode(), 'A WHERE clause is required for UPDATE.', { itemIndex });
		}

		const cols = selectedColumns.length > 0 ? selectedColumns : Object.keys(data);
		const sets = cols.map((col) => {
			const val = data[col];
			if (val === undefined || val === null) return `${col} = NULL`;
			if (typeof val === 'number' || typeof val === 'boolean') return `${col} = ${val}`;
			return `${col} = '${String(val).replace(/'/g, "''")}'`;
		});

		const sql = `UPDATE ${schema}.${tableName} SET ${sets.join(', ')} WHERE ${filter.trim()}`;
		await client.query(sql);  // DuckDB handles DML via query()
		return [{ query: sql, rows_affected: 1 }];
	}

	if (operation === 'delete') {
		const filter = exec.getNodeParameter('filter', itemIndex, '') as string;
		if (!filter || !filter.trim()) {
			throw new NodeOperationError(exec.getNode(), 'A WHERE clause is required for DELETE.', { itemIndex });
		}

		const sql = `DELETE FROM ${schema}.${tableName} WHERE ${filter.trim()}`;
		await client.query(sql);  // DuckDB handles DML via query()
		return [{ query: sql, rows_affected: 1 }];
	}

	throw new NodeOperationError(
		exec.getNode(),
		`Unknown operation: "${operation}"`,
		{ itemIndex },
	);
}

async function handleCatalog(
	exec: IExecuteFunctions,
	client: QodClient,
	itemIndex: number,
): Promise<Array<Record<string, unknown>>> {
	const operation = exec.getNodeParameter('operation', itemIndex) as string;

	switch (operation) {
		case 'listDatabases': {
			const catalogs = await client.getCatalogs();
			return catalogs.map((name) => ({ database: name }));
		}
		case 'listSchemas': {
			const tenant = exec.getNodeParameter('tenant', itemIndex) as string;
			const schemas = await client.getSchemas(tenant);
			return schemas.map((name) => ({ schema: name }));
		}
		case 'listTables': {
			const tenant = exec.getNodeParameter('tenant', itemIndex) as string;
			const schema = exec.getNodeParameter('schema', itemIndex) as string;
			const tables = await client.getTables(tenant, schema);
			return tables.map((t) => ({ schema, name: t.name, type: t.type || 'TABLE' }));
		}
		case 'describeTable': {
			const tenant = exec.getNodeParameter('tenant', itemIndex) as string;
			const schema = exec.getNodeParameter('schema', itemIndex) as string;
			const tableName = exec.getNodeParameter('table', itemIndex) as string;
			const cols = await client.getColumns(tenant, schema, tableName);
			return cols.map((col) => ({
				column: col.name,
				type: col.dataType || '',
				nullable: col.nullable !== undefined ? col.nullable : true,
			}));
		}
		default:
			throw new NodeOperationError(
				exec.getNode(),
				`Unknown catalog operation: ${operation}`,
				{ itemIndex },
			);
	}
}

// ── Node ────────────────────────────────────────────────────────────────

export class QuackOnDemand implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Quack on Demand',
		name: 'quackOnDemand',
		icon: 'file:qod.svg',
		group: ['transform'],
		version: 1,
		subtitle: '={{ $parameter["resource"] + ": " + $parameter["operation"] }}',
		description: 'Run SQL and browse catalog on a Quack on Demand FlightSQL edge',
		defaults: { name: 'Quack on Demand' },
		usableAsTool: true,
		inputs: ['main'],
		outputs: ['main'],
		credentials: [{ name: 'quackOnDemandApi', required: true }],
		properties: [
			// ── Resource ──────────────────────────────────────────────
			{
				displayName: 'Resource',
				name: 'resource',
				type: 'options',
				noDataExpression: true,
				options: [
					{ name: 'Table', value: 'table', description: 'Browse and read from a table (no SQL required)' },
					{ name: 'Query', value: 'query', description: 'Run a custom SQL statement' },
					{ name: 'Catalog', value: 'catalog', description: 'List databases, schemas, tables, or columns' },
				],
				default: 'table',
			},

			// ── Operation (right after Resource, one variant per resource) ──
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['table'] } },
				options: [
					{ name: 'Read', value: 'read', description: 'SELECT rows (auto-generated SQL)', action: 'Read rows from a table' },
					{ name: 'Insert', value: 'insert', description: 'INSERT rows (values from input JSON)', action: 'Insert rows into a table' },
					{ name: 'Update', value: 'update', description: 'UPDATE rows (values from input JSON)', action: 'Update rows in a table' },
					{ name: 'Delete', value: 'delete', description: 'DELETE rows by WHERE clause', action: 'Delete rows from a table' },
				],
				default: 'read',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['query'] } },
				options: [
					{ name: 'Execute', value: 'execute', description: 'Run a SELECT / RETURNING statement', action: 'Execute a SQL query' },
					{ name: 'Execute Update', value: 'executeUpdate', description: 'Run INSERT/UPDATE/DELETE/DDL', action: 'Execute a SQL update' },
				],
				default: 'execute',
			},
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['catalog'] } },
				options: [
					{ name: 'List Databases', value: 'listDatabases', description: 'List all available databases (tenants)', action: 'List databases' },
					{ name: 'List Schemas', value: 'listSchemas', description: 'List all schemas in the selected tenant', action: 'List schemas' },
					{ name: 'List Tables', value: 'listTables', description: 'List all tables and views in the selected schema', action: 'List tables' },
					{ name: 'Describe Table', value: 'describeTable', description: 'List columns (name, type, nullable)', action: 'Describe a table' },
				],
				default: 'listDatabases',
			},

			// ── Tenant ────────────────────────────────────────────────
			{
				displayName: 'Tenant',
				name: 'tenant',
				type: 'options',
				displayOptions: { show: { resource: ['table'] } },
				typeOptions: { loadOptionsMethod: 'getTenants' },
				default: '',
				description: 'The tenant (database) to query',
			},
			{
				displayName: 'Tenant',
				name: 'tenant',
				type: 'options',
				displayOptions: { show: { resource: ['catalog'], operation: ['listSchemas', 'listTables', 'describeTable'] } },
				typeOptions: { loadOptionsMethod: 'getTenants' },
				default: '',
				description: 'The tenant to browse',
			},

			// ── Schema ────────────────────────────────────────────────
			{
				displayName: 'Schema',
				name: 'schema',
				type: 'options',
				displayOptions: { show: { resource: ['table'] } },
				typeOptions: { loadOptionsMethod: 'getSchemas', loadOptionsDependsOn: ['tenant'] },
				default: '',
				description: 'The schema to query',
			},
			{
				displayName: 'Schema',
				name: 'schema',
				type: 'options',
				displayOptions: { show: { resource: ['catalog'], operation: ['listTables', 'describeTable'] } },
				typeOptions: { loadOptionsMethod: 'getSchemas', loadOptionsDependsOn: ['tenant'] },
				default: '',
				description: 'The schema to browse',
			},

			// ── Table ─────────────────────────────────────────────────
			{
				displayName: 'Table',
				name: 'table',
				type: 'options',
				displayOptions: { show: { resource: ['table'] } },
				typeOptions: { loadOptionsMethod: 'getTables', loadOptionsDependsOn: ['tenant', 'schema'] },
				default: '',
				description: 'The table to query',
			},
			{
				displayName: 'Table',
				name: 'table',
				type: 'options',
				displayOptions: { show: { resource: ['catalog'], operation: ['describeTable'] } },
				typeOptions: { loadOptionsMethod: 'getTables', loadOptionsDependsOn: ['tenant', 'schema'] },
				default: '',
				description: 'The table to describe',
			},

			// ── TABLE-specific fields (Read) ──────────────────────────
			{
				displayName: 'Columns',
				name: 'columns',
				type: 'multiOptions',
				displayOptions: { show: { resource: ['table'], operation: ['read'] } },
				typeOptions: { loadOptionsMethod: 'getColumns', loadOptionsDependsOn: ['tenant', 'schema', 'table'] },
				default: [],
				description: 'Columns to SELECT. Empty = all columns (SELECT *).',
			},
			{
				displayName: 'Filter (WHERE)',
				name: 'filter',
				type: 'string',
				displayOptions: { show: { resource: ['table'], operation: ['read'] } },
				default: '',
				placeholder: "c_mktsegment = 'AUTOMOBILE'",
				description: 'Optional WHERE clause (without the keyword "WHERE"). Raw SQL — do not pass untrusted user input.',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				displayOptions: { show: { resource: ['table'], operation: ['read'] } },
				typeOptions: { minValue: 0, maxValue: 100000 },
				default: 100,
				description: 'Max rows to return. 0 = no limit.',
			},

			// ── TABLE-specific fields (Insert / Update) ───────────────
				{
					displayName: 'Columns',
					name: 'columns',
					type: 'multiOptions',
					displayOptions: { show: { resource: ['table'], operation: ['insert', 'update'] } },
					typeOptions: { loadOptionsMethod: 'getColumns', loadOptionsDependsOn: ['tenant', 'schema', 'table'] },
					default: [],
					description: 'Columns to insert/update. Empty = use all keys from Values or input JSON.',
				},
				{
					displayName: 'Values (JSON)',
					name: 'valuesJson',
					type: 'json',
					displayOptions: { show: { resource: ['table'], operation: ['insert', 'update'] } },
					default: '{}',
					description:
						'Static values as JSON, e.g. {"c_name": "John", "c_acctbal": 100}. Leave empty to auto-map from the input item JSON. Expressions like {{ $json.name }} are supported.',
				},

			// ── TABLE-specific fields (Update / Delete) ───────────────
			{
				displayName: 'Filter (WHERE)',
				name: 'filter',
				type: 'string',
				displayOptions: { show: { resource: ['table'], operation: ['update', 'delete'] } },
				default: '',
				required: true,
				placeholder: "c_custkey = 42",
				description: 'WHERE clause (required for UPDATE/DELETE).',
			},

			// ── QUERY-specific fields ─────────────────────────────────
			{
				displayName: 'Query',
				name: 'query',
				type: 'string',
				typeOptions: { rows: 5 },
				displayOptions: { show: { resource: ['query'] } },
				default: '',
				placeholder: 'SELECT * FROM tpch1.customer LIMIT 100',
				required: true,
				description: 'The SQL statement to run. Runs once per input item.',
			},
		],
	};

	// ── Load Options (dynamic dropdowns) ────────────────────────────────

	methods = {
				loadOptions: {
					// List all databases / catalogs.
					async getTenants(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
						const cats = await withClient(this, (c) => c.getCatalogs());
						return cats.map((name) => ({ name, value: name }));
					},

					// List schemas inside the selected tenant.
					async getSchemas(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
						const tenant = this.getNodeParameter('tenant', 0) as string;
						if (!tenant) return [];
						const schemas = await withClient(this, (c) => c.getSchemas(tenant));
						return schemas.map((name) => ({ name, value: name }));
					},

					// List tables / views inside the selected tenant + schema.
					async getTables(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
						const tenant = this.getNodeParameter('tenant', 0) as string;
						const schema = this.getNodeParameter('schema', 0) as string;
						if (!tenant || !schema) return [];
						const tables = await withClient(this, (c) => c.getTables(tenant, schema));
						return tables.map((t) => ({ name: `${t.name} (${t.type})`, value: t.name }));
					},

					// List columns of the selected table.
					async getColumns(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
						const tenant = this.getNodeParameter('tenant', 0) as string;
						const schema = this.getNodeParameter('schema', 0) as string;
						const table = this.getNodeParameter('table', 0) as string;
						if (!tenant || !schema || !table) return [];
						const cols = await withClient(this, (c) => c.getColumns(tenant, schema, table));
						return cols.map((col) => ({
							name: `${col.name}  (${col.dataType}${col.nullable ? '' : ', NOT NULL'})`,
							value: col.name,
						}));
					},
								},
							};

	// ── Execute ────────────────────────────────────────────────────────

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const cfg = credentialsToConfig(await this.getCredentials('quackOnDemandApi'));
		const client = await QodClient.connect(cfg);
		const out: INodeExecutionData[] = [];
		const resource = this.getNodeParameter('resource', 0) as string;

		try {
			for (let i = 0; i < items.length; i++) {
				try {
					let rows: Array<Record<string, unknown>>;

					if (resource === 'query') {
						rows = await handleQuery(this, client, i);
					} else if (resource === 'table') {
						rows = await handleTable(this, client, i);
					} else {
						rows = await handleCatalog(this, client, i);
					}

					for (const row of rows) {
						out.push({ json: row as IDataObject, pairedItem: { item: i } });
					}
				} catch (error) {
					if (this.continueOnFail()) {
						out.push({
							json: { error: (error as Error).message },
							error: error as NodeOperationError,
							pairedItem: { item: i },
						} as INodeExecutionData);
						continue;
					}
					throw new NodeOperationError(this.getNode(), error as Error, { itemIndex: i });
				}
			}
		} finally {
			client.close();
		}

		return [out];
	}
}

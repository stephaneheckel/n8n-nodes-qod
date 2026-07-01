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
		return client.update(sql);
	}
	return client.query(sql);
}

async function handleTable(
	exec: IExecuteFunctions,
	client: QodClient,
	itemIndex: number,
): Promise<Array<Record<string, unknown>>> {
	const operation = exec.getNodeParameter('operation', itemIndex) as string;

	if (operation === 'read') {
		const schema = exec.getNodeParameter('schema', itemIndex) as string;
		const tableName = exec.getNodeParameter('table', itemIndex) as string;
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

	throw new NodeOperationError(
		exec.getNode(),
		`Operation "${operation}" is not yet implemented for the Table resource. Use the Query resource with a custom SQL statement for now.`,
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

			// ── TABLE operations ──────────────────────────────────────
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['table'] } },
				options: [
					{
						name: 'Read',
						value: 'read',
						description: 'SELECT rows from a table (auto-generated SQL)',
						action: 'Read rows from a table',
					},
					{
						name: 'Insert',
						value: 'insert',
						description: 'INSERT rows into a table (WIP — coming soon)',
						action: 'Insert rows into a table',
					},
					{
						name: 'Update',
						value: 'update',
						description: 'UPDATE rows in a table (WIP — coming soon)',
						action: 'Update rows in a table',
					},
					{
						name: 'Delete',
						value: 'delete',
						description: 'DELETE rows from a table (WIP — coming soon)',
						action: 'Delete rows from a table',
					},
				],
				default: 'read',
			},
			{
				displayName: 'Tenant',
				name: 'tenant',
				type: 'options',
				displayOptions: { show: { resource: ['table', 'catalog'] } },
				typeOptions: {
					loadOptionsMethod: 'getTenants',
				},
				default: '',
				description: 'The tenant (database) to query',
			},
			{
				displayName: 'Schema',
				name: 'schema',
				type: 'options',
				displayOptions: { show: { resource: ['table', 'catalog'] } },
				typeOptions: {
					loadOptionsMethod: 'getSchemas',
					loadOptionsDependsOn: ['tenant'],
				},
				default: '',
				description: 'The schema to browse',
			},
			{
				displayName: 'Table',
				name: 'table',
				type: 'options',
				displayOptions: { show: { resource: ['table', 'catalog'] } },
				typeOptions: {
					loadOptionsMethod: 'getTables',
					loadOptionsDependsOn: ['tenant', 'schema'],
				},
				default: '',
				description: 'The table to query',
			},
			{
				displayName: 'Columns',
				name: 'columns',
				type: 'multiOptions',
				displayOptions: {
					show: { resource: ['table'], operation: ['read'] },
				},
				typeOptions: {
					loadOptionsMethod: 'getColumns',
					loadOptionsDependsOn: ['tenant', 'schema', 'table'],
				},
				default: [],
				description: 'Columns to include in the SELECT. Leave empty for all columns (SELECT *).',
			},
			{
				displayName: 'Filter (WHERE)',
				name: 'filter',
				type: 'string',
				displayOptions: {
					show: { resource: ['table'], operation: ['read'] },
				},
				default: '',
				placeholder: "c_mktsegment = 'AUTOMOBILE'",
				description:
					'Optional WHERE clause (without the keyword "WHERE"). Example: c_acctbal > 0 AND c_name LIKE \'A%\'. Raw SQL — do not pass untrusted user input.',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				displayOptions: {
					show: { resource: ['table'], operation: ['read'] },
				},
				typeOptions: { minValue: 0, maxValue: 100000 },
				default: 100,
				description: 'Maximum number of rows to return. Set to 0 for no limit.',
			},

			// ── QUERY operations ──────────────────────────────────────
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['query'] } },
				options: [
					{
						name: 'Execute',
						value: 'execute',
						description: 'Run a SELECT / RETURNING statement and return the result rows',
						action: 'Execute a SQL query',
					},
					{
						name: 'Execute Update',
						value: 'executeUpdate',
						description: 'Run an INSERT, UPDATE, DELETE, or DDL statement',
						action: 'Execute a SQL update',
					},
				],
				default: 'execute',
			},
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

			// ── CATALOG operations ────────────────────────────────────
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['catalog'] } },
				options: [
					{
						name: 'List Databases',
						value: 'listDatabases',
						description: 'List all available databases (tenants)',
						action: 'List databases',
					},
					{
						name: 'List Schemas',
						value: 'listSchemas',
						description: 'List all schemas in the selected tenant',
						action: 'List schemas',
					},
					{
						name: 'List Tables',
						value: 'listTables',
						description: 'List all tables and views in the selected schema',
						action: 'List tables',
					},
					{
						name: 'Describe Table',
						value: 'describeTable',
						description: 'List columns (name, type, nullable) for the selected table',
						action: 'Describe a table',
					},
				],
				default: 'listDatabases',
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

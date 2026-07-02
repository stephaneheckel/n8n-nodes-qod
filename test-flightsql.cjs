const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const protobuf = require('protobufjs');
const { tableFromIPC } = require('apache-arrow');

const FLIGHT_PROTO = `
syntax = "proto3";
package arrow.flight.protocol;
service FlightService { rpc GetFlightInfo(FlightDescriptor) returns (FlightInfo) {} rpc DoGet(Ticket) returns (stream FlightData) {} }
message FlightDescriptor { enum DescriptorType { UNKNOWN = 0; PATH = 1; CMD = 2; } DescriptorType type = 1; bytes cmd = 2; repeated string path = 3; }
message Ticket { bytes ticket = 1; }
message FlightEndpoint { Ticket ticket = 1; }
message FlightInfo { repeated FlightEndpoint endpoint = 3; }
message FlightData { bytes data_header = 2; bytes data_body = 1000; }
`;

const FLIGHTSQL_PROTO = `
syntax = "proto3";
package arrow.flight.protocol.sql;
message Any { string type_url = 1; bytes value = 2; }
message CommandGetDbSchemas { optional string catalog = 1; }
message CommandGetTables { optional string catalog = 1; optional string db_schema_filter_pattern = 2; bool include_schema = 5; }
`;

function encapsulate(header, body) {
  const padded = (header.length + 7) & ~7;
  const prefix = Buffer.alloc(8 + padded);
  prefix.writeUInt32LE(0xffffffff, 0);
  prefix.writeInt32LE(padded, 4);
  header.copy(prefix, 8);
  return Buffer.concat([prefix, body]);
}
const EOS = Buffer.from([0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00]);

function buildCmd(type, payload, typeName) {
  const inner = type.encode(payload).finish();
  const anyType = root.lookupType('arrow.flight.protocol.sql.Any');
  return anyType.encode({ typeUrl: 'type.googleapis.com/arrow.flight.protocol.sql.' + typeName, value: inner }).finish();
}

const root = new protobuf.Root();
protobuf.parse(FLIGHT_PROTO, root);
protobuf.parse(FLIGHTSQL_PROTO, root);

const pkgDef = protoLoader.fromJSON(root.toJSON(), {keepCase:true,longs:String,enums:String,defaults:true,oneofs:true});
const proto = grpc.loadPackageDefinition(pkgDef);
const FlightService = proto.arrow.flight.protocol.FlightService;
const client = new FlightService('127.0.0.1:31337', grpc.credentials.createInsecure());
const md = new grpc.Metadata();
md.set('authorization', 'Basic ' + Buffer.from('flight_username:flight_password').toString('base64'));

async function testCmd(label, type, payload, typeName) {
  return new Promise((resolve) => {
    const cmd = Buffer.from(buildCmd(type, payload, typeName));
    client.GetFlightInfo({ type: 'CMD', cmd }, md, (err, info) => {
      if (err) { console.log(label + ': ERROR', err.code, err.details || err.message); resolve(); return; }
      const eps = info.endpoint || [];
      if (eps.length === 0) { console.log(label + ': no endpoints'); resolve(); return; }
      const messages = [];
      const stream = client.DoGet({ ticket: eps[0].ticket.ticket }, md);
      stream.on('data', (fd) => {
        const h = fd.dataHeader || fd.data_header || Buffer.alloc(0);
        const b = fd.dataBody || fd.data_body || Buffer.alloc(0);
        if (h.length > 0) messages.push(encapsulate(h, b));
      });
      stream.on('end', () => {
        const table = tableFromIPC(Buffer.concat([...messages, EOS]));
        const rows = table.toArray().map(r => r.toJSON()).slice(0,5);
        console.log(label + ': OK, rows=' + table.numRows + ' ' + JSON.stringify(rows));
        resolve();
      });
      stream.on('error', (e) => { console.log(label + ': STREAM ERROR', e.details); resolve(); });
    });
  });
}

(async () => {
  const sch = root.lookupType('arrow.flight.protocol.sql.CommandGetDbSchemas');
  const tbl = root.lookupType('arrow.flight.protocol.sql.CommandGetTables');

  await testCmd('getSchemas(TPC-H-small)', sch, { catalog: 'TPC-H-small' }, 'CommandGetDbSchemas');
  await testCmd('getSchemas(system)', sch, { catalog: 'system' }, 'CommandGetDbSchemas');
  await testCmd('getSchemas(temp)', sch, { catalog: 'temp' }, 'CommandGetDbSchemas');
  await testCmd('getTables(no filter)', tbl, { catalog: 'TPC-H-small' }, 'CommandGetTables');
  await testCmd('getTables(main/main)', tbl, { catalog: 'TPC-H-small', dbSchemaFilterPattern: 'main' }, 'CommandGetTables');
  await testCmd('getTables(+schema)', tbl, { catalog: 'TPC-H-small', includeSchema: true }, 'CommandGetTables');
  client.close();
})();

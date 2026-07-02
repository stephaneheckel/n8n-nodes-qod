const grpc = require('@grpc/grpc-js');
const protobuf = require('protobufjs');
const protoLoader = require('@grpc/proto-loader');

const FLIGHT_PROTO = `
syntax = "proto3";
package arrow.flight.protocol;
service FlightService {
  rpc Handshake(HandshakeRequest) returns (HandshakeResponse) {}
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

const root = new protobuf.Root();
protobuf.parse(FLIGHT_PROTO, root);
const pkgDef = protoLoader.fromJSON(root.toJSON(), {keepCase:true,longs:String,enums:String,defaults:true,oneofs:true});
const proto = grpc.loadPackageDefinition(pkgDef);
const FlightService = proto.arrow.flight.protocol.FlightService;
const client = new FlightService('127.0.0.1:31337', grpc.credentials.createInsecure());

const md = new grpc.Metadata();
const basic = Buffer.from('gizmosql_user:password').toString('base64');
md.set('authorization', 'Basic ' + basic);

client.Handshake({ payload: Buffer.from('gizmosql_user:password'), protocolVersion: 0 }, md, (err, resp) => {
  if (err) { console.log('FAIL:', err.code, err.details || err.message); process.exit(1); }
  const rx = root.lookupType('arrow.flight.protocol.HandshakeResponse');
  const decoded = rx.decode(resp.payload);
  const token = Buffer.from(decoded.payload || '').toString('utf8');
  console.log('OK, token:', token.slice(0, 50) + '...');
  client.close();
});

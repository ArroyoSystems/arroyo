// adapted from https://lucas-levin.com/code/blog/mocking-grpc-web-requests-for-integration-testing

import { ResponseTransformer } from 'msw';
import { Message } from '@bufbuild/protobuf';

export const createGrpcResponse: (data: Message) => any = data => {
  const d = data.toBinary();
  // create the data length bytes - there is probably a more concise way, but this works
  const dataLengthBytes = new Uint8Array(new Uint32Array([d.byteLength]).buffer);
  dataLengthBytes.reverse();
  const dataFrame = new Uint8Array(d.byteLength + 5);
  dataFrame.set([0x00], 0); // set the magic byte 0x00 to identify the data frame
  dataFrame.set(dataLengthBytes, 1); // set the length bytes
  dataFrame.set(d, 5); // set the actual data

  // you can add mock errors by tweaking the trailers string with different status codes/messages
  const trailersString = `grpc-status: 0\r\ngrpc-message: `;
  const encoder = new TextEncoder();
  const trailers = encoder.encode(trailersString);
  const trailersLengthBytes = new Uint8Array(new Uint32Array([trailers.byteLength]).buffer);
  trailersLengthBytes.reverse();
  const trailersFrame = new Uint8Array(trailers.byteLength + 5);
  trailersFrame.set([0x80], 0); // magic byte for trailers is 0x80
  trailersFrame.set(trailersLengthBytes, 1);
  trailersFrame.set(trailers, 5);

  // create the final body by combining the data frame and trailers frame
  const body = new Uint8Array(dataFrame.byteLength + trailersFrame.byteLength);
  body.set(dataFrame, 0);
  body.set(trailersFrame, dataFrame.byteLength);

  return {
    statusCode: 200,
    body: body.buffer,
    headers: {
      'content-type': 'application/grpc-web+proto',
    },
  };
};

export const grpcRes: (
  grpcResponse: ReturnType<typeof createGrpcResponse>
) => ResponseTransformer = grpcResponse => {
  return res => {
    res.body = grpcResponse.body;
    Object.entries(grpcResponse.headers).forEach(([key, value]) =>
      res.headers.set(key, value as string)
    );
    res.status = grpcResponse.statusCode;
    return res;
  };
};

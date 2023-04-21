import { useEffect, useState } from "react";
import { OutputData, GrpcOutputSubscription, RefreshSampleReq } from "../../gen/api_pb";
import { ApiClient } from "../../main";
import { PipelineOutputs } from "../pipelines/JobOutputs";
import { useParams } from "react-router-dom";
import { Box, Button, Container, Flex, IconButton, Spacer, Spinner, Text, VStack } from "@chakra-ui/react";

export function SourcePreview({ client }: { client: ApiClient }) {
    const [loading, setLoading] = useState(false);
    const [outputs, setOutputs] = useState<Array<{ id: number, data: OutputData }>>([]);

    let { jobId } = useParams();

    const refreshSample = async () => {
        setLoading(true);
        // start the pipeline
        let resp = await (await client()).refreshSample(new RefreshSampleReq({
            sourceId: BigInt(24),
        }));
        setLoading(false);
        setOutputs(resp.rando.map((r, i) => ({ id: i, data: r })));
    }

    // return a PipelineOutput with output from the source raw pipeline
    return (
        <Box>
            <VStack>
                <Flex w="100%">
                    <Box p={5}>
                        <Text fontSize={20}>
                            Source: verrrrry_slow
                        </Text>
                    </Box>
                    <Spacer />
                    <Box p={5}>
                        <Button isLoading={loading} loadingText="Refreshing" onClick={() => refreshSample()}>
                            Refresh
                        </Button>
                    </Box>
                </Flex>
                <Flex>
                    {!loading && outputs.length > 0 ? <PipelineOutputs outputs={outputs} /> : <Box />}
                </Flex>
            </VStack>
        </Box>
    )
}

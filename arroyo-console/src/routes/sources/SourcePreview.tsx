import { useEffect, useState } from "react";
import { OutputData, GrpcOutputSubscription, RefreshSampleReq } from "../../gen/api_pb";
import { ApiClient } from "../../main";
import { PipelineOutputs } from "../pipelines/JobOutputs";
import { useParams } from "react-router-dom";
import { Box, Button, IconButton } from "@chakra-ui/react";

export function SourcePreview({ client }: { client: ApiClient }) {
    const [subscribed, setSubscribed] = useState(false);
    // loading
    const [loading, setLoading] = useState(false);
    const [outputs, setOutputs] = useState<Array<{ id: number, data: OutputData }>>([]);

    let { jobId } = useParams();

    const subscribe = async () => {
        if (subscribed || loading) {
            return;
        }

        setSubscribed(true);

        let row = 1;
        for await (const res of (await client()).subscribeToOutput(
            new GrpcOutputSubscription({
                jobId: jobId,
            })
        )) {
            setOutputs(outputs => {
                let newOutputs = [...outputs, { id: row++, data: res }];
                if (newOutputs.length > 20) {
                    newOutputs.shift();
                }
                return newOutputs;
            });
        }
    }

    const refreshSample = async () => {
        setLoading(true);
        // start the pipeline
        await (await client()).refreshSample(new RefreshSampleReq({
            sourceId: BigInt(24),
        }));
    }

    // return a PipelineOutput with output from the source raw pipeline
    return (
        <Box>
            {/* icon button with the refresh icon */}
            {/* <IconButton
                aria-label="Refresh"
                icon={<RefreshIcon />}
                onClick={ }
            /> */}
            {outputs.length > 0 ?
                (<PipelineOutputs outputs={outputs} />)
                : (<Button onClick={refreshSample}>Refresh</Button>)}
        </Box>
    )
}

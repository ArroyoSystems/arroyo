import { useEffect, useState } from "react";
import { OutputData, GrpcOutputSubscription } from "../../gen/api_pb";
import { ApiClient } from "../../main";
import { PipelineOutputs } from "../pipelines/JobOutputs";
import { useParams } from "react-router-dom";
import { Button } from "@chakra-ui/react";

export function SourcePreview({ client }: { client: ApiClient }) {
    const [subscribed, setSubscribed] = useState(false);
    const [outputs, setOutputs] = useState<Array<{ id: number, data: OutputData }>>([]);

    let { jobId } = useParams();

    const subscribe = async () => {
        if (subscribed) {
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

    // return a PipelineOutput with output from the source raw pipeline
    return outputs.length > 0 ?
        (<PipelineOutputs outputs={outputs} />)
        : (<Button onClick={subscribe}>Subscribe</Button>)
}

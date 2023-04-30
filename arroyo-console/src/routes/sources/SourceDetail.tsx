import { useParams } from "react-router-dom";
import { ApiClient } from "../../main";
import { Divider, Spinner, VStack, Flex, Text, Spacer, Box, AccordionItem, AccordionButton, AccordionPanel, AccordionIcon, Button, Badge, Center, HStack } from "@chakra-ui/react";
import { PipelineOutputs } from "../pipelines/JobOutputs";
import { useEffect, useState } from "react";
import { BuiltinSink, JobDetailsReq, OutputData, SourceDef } from "../../gen/api_pb";
import { Catalog } from "../pipelines/Catalog";
import { GrpcOutputSubscription, CreatePipelineReq, CreateSqlJob } from "../../gen/api_pb";

function PipelinePreview({ client, jobId, subscribed, setSubscribed, setJobState }: { client: ApiClient, jobId: string, subscribed: boolean, setSubscribed: (b: boolean) => void, setJobState: (s: string | undefined) => void }) {
    let [outputs, setOutputs] = useState<Array<{ id: number, data: OutputData }>>([]);

    const subscribe = async () => {
        let row = 1;
        for await (const res of (await client()).subscribeToOutput(
            new GrpcOutputSubscription({
                jobId: jobId,
            })
        )) {
            setOutputs(prevState => {
                let lastId = prevState.length == 0 ? 0 : prevState[prevState.length - 1].id;
                let newOutputs = [...prevState, { id: lastId + 1, data: res }]
                if (prevState.length > 20) {
                    newOutputs.shift();
                }
                return newOutputs;
            });
        }
        setSubscribed(false);
    }

    const blockUntilJobReady = async () => {
        let job = await (await client()).getJobDetails(new JobDetailsReq({ jobId: jobId }));
        let jobState = job.jobStatus?.state;
        setJobState(jobState);
        if (job.jobStatus?.state != "Running") {
            await new Promise(r => setTimeout(r, 1000));
            await blockUntilJobReady();
        }
    }

    useEffect(() => {
        setSubscribed(true);
        blockUntilJobReady().then(subscribe);
    }, [jobId]);

    return (
        <Box>
            <PipelineOutputs outputs={outputs} />
        </Box>
    )
}

export function SourceDetail({ client }: { client: ApiClient }) {
    let [source, setSource] = useState<SourceDef | null>(null);
    let [previewJobId, setPreviewJobId] = useState<string | null>(null);
    let [subscribed, setSubscribed] = useState(false);
    let [jobState, setJobState] = useState<string | undefined>(undefined);


    let { id } = useParams();

    useEffect(() => {
        // initialize the source given the ID
        const fetchData = async () => {
            const source = await (await client()).getSource({ id: BigInt(id!) });

            setSource(source.source!);
        }
        fetchData()
    }, []);

    // log a message when subscribed changes
    useEffect(() => {
        console.log(`subscribed: ${subscribed}`);
    }, [subscribed]);

    const tail = async () => {
        let jobResp = await (await client()).previewPipeline(new CreatePipelineReq({
            name: source!.name,
            config: {
                case: "sql",
                value: new CreateSqlJob({
                    query: `SELECT * FROM ${source!.name};`,
                    sink: { case: "builtin", value: BuiltinSink.Web }
                }),
            },
        }));
        setPreviewJobId(jobResp.jobId);
    }

    return (
        <Box>
            <VStack>
                <Flex w="100%" p="5px">
                    <Text fontSize="x-large">Source {source?.name}</Text>
                    <Spacer />
                    <Center><Badge mr={3}>{jobState == null ? "Not Running" : jobState}</Badge></Center>
                    <Button onClick={tail} isLoading={subscribed} loadingText="In Progress" >Tail</Button>
                </Flex>
                <Divider orientation="horizontal" />
                {/* flex with pipeline outputs and a column of text */}
                <Box>
                    <Flex w="100%">
                        <Box w="calc(100vw - 600px)">
                            {previewJobId ? <PipelinePreview client={client} jobId={previewJobId} subscribed setSubscribed={setSubscribed} setJobState={setJobState} /> : <Box />}
                        </Box>
                        <Spacer />
                        <Box w="400px" p="5">
                            <Text>Source Type: {source?.sourceType.case}</Text>
                            {source == null ? null : <Catalog sources={[source]} />}
                            <Text>Consumers: {source?.consumers}</Text>
                            <Text>Connection: {source?.connection != null ? source?.connection : "n/a"}</Text>
                        </Box>
                    </Flex>
                </Box>
            </VStack>

        </Box >
    )
}

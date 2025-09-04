import { JSONSchema7 } from 'json-schema';
import React, { useRef, useState } from 'react';
import {
  AlertDialog,
  AlertDialogBody,
  AlertDialogContent,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogOverlay,
  Button,
  useDisclosure,
  useToast,
} from '@chakra-ui/react';
import { ConnectionProfile, Connector, post } from '../../lib/data_fetching';
import { formatError } from '../../lib/util';
import { JsonForm } from './JsonForm';

export const CreateProfile = ({
  connector,
  addConnectionProfile,
  next,
}: {
  connector: Connector;
  addConnectionProfile: (c: ConnectionProfile) => void;
  next: (id: string) => void;
}) => {
  const [error, setError] = useState<string | null>(null);
  const [valid, setValid] = useState<boolean | null>(null);
  const [validating, setValidating] = useState<boolean>(false);
  const state = useRef<any | null>(null);
  const toast = useToast();
  const { isOpen, onOpen, onClose } = useDisclosure();
  const cancelRef = useRef<any>();

  const schema = JSON.parse(connector.connection_config!) as JSONSchema7;
  const validate = async (d: any) => {
    setValidating(true);
    const { data, error } = await post('/v1/connection_profiles/test', {
      body: {
        name: d.name,
        connector: connector.id,
        config: d,
      },
    });

    if (error || data.error) {
      setError(data?.message || 'Something went wrong');
      setValid(false);
    } else {
      toast({
        title: 'Profile validated',
        description: `Successfully validated connection profile ${d.name}`,
        status: 'success',
        duration: 5000,
        isClosable: true,
        position: 'top',
      });
    }

    setValidating(false);
    let valid = !(error == true || data.error);
    setValid(valid);
  };

  const onSubmit = async (d: any) => {
    if (valid == null) {
      await validate(d);
      return;
    }
    state.current = d;

    if (!valid) {
      onOpen();
    } else {
      await submit();
    }
  };

  const submit = async () => {
    if (state == null) {
      return;
    }

    setError(null);
    const { data: connectionProfile, error } = await post('/v1/connection_profiles', {
      body: {
        name: state.current.name,
        connector: connector.id,
        config: state.current,
      },
    });

    if (connectionProfile) {
      addConnectionProfile(connectionProfile);
    }

    if (error) {
      setError(formatError(error));
      return;
    }

    next(connectionProfile.id);
  };

  return (
    <>
      <JsonForm
        schema={schema}
        hasName={true}
        error={error}
        initial={{}}
        button={valid == null ? 'Validate' : 'Create'}
        buttonColor={valid == null ? 'blue' : valid ? 'green' : 'red'}
        onSubmit={onSubmit}
        onChange={values => {
          if (JSON.stringify(values) != JSON.stringify(state)) {
            setError(null);
            setValid(null);
          }
          state.current = values;
        }}
        inProgress={validating}
      />
      <AlertDialog isOpen={isOpen} leastDestructiveRef={cancelRef} onClose={onClose}>
        <AlertDialogOverlay>
          <AlertDialogContent>
            <AlertDialogHeader fontSize="lg" fontWeight="bold">
              Validation failed
            </AlertDialogHeader>

            <AlertDialogBody>
              We were not able to validate that the connection is correctly configured. You may
              continue creating it, but may encounter issues.
            </AlertDialogBody>

            <AlertDialogFooter>
              <Button ref={cancelRef} onClick={onClose}>
                Go Back
              </Button>
              <Button
                colorScheme="red"
                onClick={() => {
                  onClose();
                  submit();
                }}
                ml={3}
              >
                Create
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialogOverlay>
      </AlertDialog>
    </>
  );
};

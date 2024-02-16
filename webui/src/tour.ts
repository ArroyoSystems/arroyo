import React, { useState } from 'react';

export const enum TourSteps {
  WelcomeModal = 'WelcomeModal',
  CreatePipelineButton = 'CreatePipelineButton',
  CreatePipelineModal = 'CreatePipelineModal',
  ExampleQueriesButton = 'ExampleQueriesButton',
  ExampleQuery = 'ExampleQuery',
  Preview = 'Preview',
  TourCompleted = 'TourCompleted',
}

export const TourContext = React.createContext<{
  tourActive: boolean;
  tourStep: TourSteps | undefined;
  setTourStep: (tourStep: TourSteps | undefined) => void;
  disableTour: () => void;
}>({
  tourActive: false,
  tourStep: undefined,
  setTourStep: _ => {
    throw new Error('setTourStep must be used within a TourContextProvider');
  },
  disableTour: () => {
    throw new Error('disableTour must be used within a TourContextProvider');
  },
});

export const getTourContextValue = () => {
  const [tourStep, setTourStep] = useState<TourSteps | undefined>(undefined);

  const tourActive = !window.localStorage.getItem('tourDisabled');

  const disableTour = () => {
    setTourStep(undefined);
    window.localStorage.setItem('tourDisabled', 'true');
  };

  const setTourStepWithDelay = (newTourStep: TourSteps | undefined) => {
    if (!tourActive) {
      return;
    }

    if (tourStep === newTourStep) {
      return;
    }

    setTourStep(undefined);
    setTimeout(() => setTourStep(newTourStep), 800);
  };

  return { tourActive, tourStep, setTourStep: setTourStepWithDelay, disableTour };
};

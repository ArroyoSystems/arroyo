const ibmCloudValidationRules = require('@ibm-cloud/openapi-ruleset');
const { propertyCasingConvention } = require('@ibm-cloud/openapi-ruleset/src/functions');
const { schemas } = require('@ibm-cloud/openapi-ruleset-utilities/src/collections');

module.exports = {
  extends: ibmCloudValidationRules,
  rules: {
    'ibm-property-casing-convention': {
      description: 'Property names must follow camelCase',
      message: '{{error}}',
      resolved: true,
      given: schemas,
      severity: 'warn',
      then: {
        function: propertyCasingConvention,
        functionOptions: {
          type: 'camel'
        }
      }
    }
  }
};

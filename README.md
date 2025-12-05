# VantaIntegrationsSamplePythonLoaders
This is a repository that contains sample python scripts that will allow you to programmatically send data to Vanta via the Vanta Integrations API


Custom Resource


python3 CustomResourceLoader.py SampleCustomResource.csv https://api.vanta.com/v1/resources/custom_resource --auth-token <token> --resource-id <customResource_resource_id>


UserAccount Resource


 python3 UserLoader.py SampleUserResource.csv https://api.vanta.com/v1/resources/user_account --auth-token <token>i --resource-id <userAccount_resource_id>



Vulnerability Component Resource


python3 VulnerabilityLoader.py VulnerabilityResource.csv https://api.vanta.com/v1/resources/vulnerable_component --auth-token <token> --resource-id <vuln_compoent_resource_id>

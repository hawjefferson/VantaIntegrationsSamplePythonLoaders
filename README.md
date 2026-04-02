# VantaIntegrationsSamplePythonLoaders
This is a repository that contains sample python scripts that will allow you to programmatically send data to Vanta via the Vanta Integrations API


<br>Custom Resource


python3 CustomResourceLoader.py SampleCustomResource.csv https://api.vanta.com/v1/resources/custom_resource --auth-token <token> --resource-id <customResource_resource_id>


<br>UserAccount Resource


 python3 UserLoader.py SampleUserResource.csv https://api.vanta.com/v1/resources/user_account --auth-token <token>i --resource-id <userAccount_resource_id>



<br>Vulnerability Component Resource


python3 VulnerabilityLoader.py VulnerabilityResource.csv https://api.vanta.com/v1/resources/vulnerable_component --auth-token <token> --resource-id <vuln_compoent_resource_id>

<br>If you're getting no module found, within the working folder path

python3 -m venv my_test_env <br>
source my_test_env/bin/activate <br>
pip install requests <br>

<br>External Training Resource

python3 SecurityTrainingLoader.py SampleSecurityTrainingResource.csv https://api.vanta.com/v1/resources/user_security_training_status --auth-token <token> --resource-id <training_resource_id>


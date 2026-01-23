# Creamos los archivos main.tf, provider.tf, variables.tf, terraform.tfvars
# Añadir credenciales terraform.json al codespaces
mkdir -p /home/codespace/.gcp
v terraform.json /home/codespace/.gcp/terraform.json
ls -la /home/codespace/.gcp
export GOOGLE_APPLICATION_CREDENTIALS="/home/codespace/.gcp/terraform.json"


# Actualizar paquetes básicos
sudo apt-get update
sudo apt-get install -y gnupg software-properties-common curl

# Añadir el repositorio oficial de HashiCorp
curl -fsSL https://apt.releases.hashicorp.com/gpg \
  | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
| sudo tee /etc/apt/sources.list.d/hashicorp.list

# Instalar Terraform
sudo apt-get update
sudo apt-get install -y terraform
terraform version
terraform init
terraform plan
terraform apply
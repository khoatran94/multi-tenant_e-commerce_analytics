from prefect import flow, task
from prefect_dbt.cli.commands import DbtCoreOperation

# Define the list of tenants you want to process
TENANTS = ["amazon", "etsy", "shopify"]

@task
def run_dbt_for_tenant(tenant_id: str):
    """
    Runs dbt commands for a specific tenant ID.
    """
    # Define the dbt project and profiles directories
    project_dir = "~/multi-tenant_e-commerce_analytics/ecommerce_analytics" # Assumes the script is in your dbt project root
    # Replace with your actual profiles directory path if it's not the default
    profiles_dir = "~/.dbt" 

    # Define the commands to run, passing the tenant_id as a dbt variable
    commands = [
        f'dbt run --select staging --vars "tenant_id: {tenant_id}"',
        f'dbt run --select intermediate --vars "tenant_id: {tenant_id}"',
        f'dbt run --select analytics --vars "tenant_id: {tenant_id}"'
    ]

    # Execute all commands sequentially for the given tenant
    for command in commands:
        DbtCoreOperation(
            commands=[command],
            project_dir=project_dir,
            profiles_dir=profiles_dir
        ).run()

@flow(name="Multi-Tenant dbt Run")
def multi_tenant_dbt_flow():
    """
    Main flow that iterates through all tenants and runs dbt commands for each.
    """
    for tenant in TENANTS:
        # Call the task for each tenant
        run_dbt_for_tenant(tenant)

if __name__ == "__main__":
    multi_tenant_dbt_flow.serve(
        name="multi_tenant_deployment",
        cron="0 3 * * *"
    )

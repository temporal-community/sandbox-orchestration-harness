"""An OpenAI Agents SDK example that calls the Go Nexus sandbox service."""

import asyncio
import os
from datetime import timedelta
from uuid import uuid4

from agents import Agent, Runner, function_tool
from temporalio.client import Client

from generated.sandbox_contract import CreateInput, ExecuteInput, SandboxControl, SandboxHandle


async def main():
    client = await Client.connect(
        os.getenv("TEMPORAL_HOST_PORT", "localhost:7233"),
        namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
        api_key=os.getenv("TEMPORAL_API_KEY"),
        tls=bool(os.getenv("TEMPORAL_API_KEY")),
    )
    nexus = client.create_nexus_client(
        SandboxControl, os.getenv("SANDBOX_NEXUS_ENDPOINT", "sandbox-control")
    )
    sandbox = SandboxHandle(sandbox_id=f"sandbox-{uuid4()}")
    # Wait for provisioning to finish. The sandbox workflow stays running.
    await nexus.execute_operation(
        SandboxControl.create,
        CreateInput(sandbox_id=sandbox.sandbox_id, profile=os.getenv("SANDBOX_PROFILE", "default")),
        id=f"create-{sandbox.sandbox_id}",
        schedule_to_close_timeout=timedelta(minutes=15),
    )

    @function_tool
    async def run_shell(command: str) -> str:
        """Run a shell command in the isolated sandbox and return its output."""
        # Wait for the command result. Nexus delivers it through a completion callback.
        result = await nexus.execute_operation(
            SandboxControl.execute,
            ExecuteInput(sandbox_id=sandbox.sandbox_id, command=command),
            id=str(uuid4()),
            schedule_to_close_timeout=timedelta(minutes=15),
        )
        return f"exit={result.exit_code}\n{result.stdout}{result.stderr}"

    try:
        # Run the agent in this Python process. Temporal runs the sandbox operations.
        result = await Runner.run(
            Agent(name="Sandbox agent", instructions="Use run_shell to answer.", tools=[run_shell]),
            "Run python -c 'print(6 * 7)' and tell me the result.",
        )
        print(result.final_output)
    finally:
        # Send the existing stop signal and wait for workflow cleanup to finish.
        await nexus.execute_operation(
            SandboxControl.delete, sandbox, id=f"delete-{sandbox.sandbox_id}",
            schedule_to_close_timeout=timedelta(minutes=15),
        )


if __name__ == "__main__":
    asyncio.run(main())

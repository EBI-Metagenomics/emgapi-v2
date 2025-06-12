from importlib.resources import files

from activate_django_first import EMG_CONFIG
from workflows.prefect_utils.build_cli_command import cli_command
from workflows import ena_utils


def test_cli_command_generator():
    command = cli_command(["echo", "hello", "world"])
    assert command == "echo hello world"

    command = cli_command(["echo", "--message=hello world"])
    assert command == "echo '--message=hello world'"

    command = cli_command(["echo", '--message="hello world"'])
    assert command == "echo '--message=\"hello world\"'"

    command = cli_command(["echo", "--message=helloworld"])
    assert command == "echo --message=helloworld"

    some_value = "world"
    command = cli_command(["echo", f"hello {some_value}"])
    assert command == "echo 'hello world'"

    some_condition = False
    command = cli_command(["echo", "hello", some_condition and "world"])
    assert command == "echo hello"

    some_condition = True
    command = cli_command(["echo", "--greeting=hello", some_condition and "--world"])
    assert command == "echo --greeting=hello --world"

    command = cli_command(["echo", ("--greeting", "hello", "-world")])
    assert command == "echo --greeting hello -world"


def test_cli_command_generator_realistic():
    # More realistic test with complicated completions for a webin-cli command

    xms = int(EMG_CONFIG.assembler.assembly_uploader_mem_gb / 2)
    xmx = int(EMG_CONFIG.assembler.assembly_uploader_mem_gb)

    logback_config = files(ena_utils) / "webincli_logback.xml"

    manifest = "my-files/manifest.txt"
    username = "Webin-me"
    password = "not=a&pw"
    dry_run = False

    command = cli_command(
        [
            ("java", f"-Dlogback.configurationFile={logback_config}"),
            f"-Xms{xms}g",
            f"-Xmx{xmx}g",
            ("-jar", EMG_CONFIG.webin.webin_cli_executor),
            ("-context", "genome"),
            ("-manifest", manifest),
            ("-userName", username),
            ("-password", password),
            EMG_CONFIG.webin.aspera_ascp_executor and "-ascp",
            dry_run and "-test -validate",
            not dry_run and "-submit",
        ]
    )
    assert (
        command
        == "java -Dlogback.configurationFile=/app/workflows/ena_utils/webincli_logback.xml -Xms2g -Xmx4g -jar /usr/bin/webin-cli/webin-cli.jar -context genome -manifest my-files/manifest.txt -userName Webin-me -password 'not=a&pw' -ascp -submit"
    )

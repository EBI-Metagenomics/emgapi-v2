from workflows.prefect_utils.build_cli_command import cli_command


def test_cli_command_generator():
    command = cli_command(["echo", "hello", "world"])
    assert command == "echo hello world"

    command = cli_command(["echo", "--message=hello world"])
    assert command == "echo '--message=hello world'"

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

#!/bin/zsh

function runCommand() {
    for d in ./gender*/ ; do /bin/zsh -c "(cd "$d" && "$@")"; done
}

runCommand "python ../../scripts/run_bootstrap.py"
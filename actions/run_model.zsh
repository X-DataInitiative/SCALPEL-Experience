#!/bin/zsh

function runCommand() {
    for d in ./G*/ ; do /bin/zsh -c "(cd "$d" && "$@")"; done
}

runCommand "python ../../scripts/run_model.py"
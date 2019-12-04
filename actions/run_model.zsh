#!/bin/zsh

function runCommand() {
    for d in ./G*/ ; do /bin/zsh -c "(cd "$d" && "$@")"; done
}

runCommand "python ../../actions/run_model.py"
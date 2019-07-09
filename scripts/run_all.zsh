#!/bin/zsh

function runCommand() {
    for d in ./gender*/ ; do /bin/zsh -c "(cd "$d" && source activate pandas && "$@" &)"; done
}

runCommand "python ../../scripts/run_model.py"
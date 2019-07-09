#!/bin/zsh

function runCommand() {
    for d in ./gender*/ ; do /bin/zsh -c "(cd "$d" && source activate CMAP && "$@" &)"; done
}

runCommand "python ../produce_ci.py"

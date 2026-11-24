**Moved to [zachsents/switch](https://github.com/zachsents/switch/tree/main/extensions/texts).** This repo is archived.

# switch-ext-texts

Every text both ways, and sends that report delivered and read.

A [Switch](https://switch.ax) extension: a Python MCP server (`sbmcp.py`, a Switchboard appendage) that Switch runs through
[switch-ext-kit](https://github.com/zachsents/switch-ext-kit)'s shim. Its messages carry who sent them, for Switch's inbox and Contacts.

## Install

Ask Switch's agent to install `github:zachsents/switch-ext-texts`, or add it in the desktop's Extensions view (⌘K, Extensions). Switch runs it
on the first machine with what it needs (`messages`).

## Setup

Runs on a Mac signed in to Messages, which must provide `messages`. Its process needs Full Disk Access to read Messages' database.

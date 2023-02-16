# cms-htcondor-es

> For detailed deployment, please refer to [scripts/PROD_TEST_MIGRATION_RECOVERY.md](scripts/PROD_TEST_MIGRATION_RECOVERY.md)

In order to update the code base in virtual machine,

- Please push your commits or open your PRs against "https://github.com/dmwm/cms-htcondor-es/tree/master".

- In virtual machine, fetch: `git fetch --all`

- Check the difference between local and remote to be sure: `git diff origin/master master`

- Before rebase, please make sure if 12m period is finished:

    - No python spider_cms process: `ps aux | grep "python spider_cms.py"`

    - And `tail -f log/spider_cms.log` is like `@@@ Total processing time:...`

- Finally rebase `git pull --rebase origin master`

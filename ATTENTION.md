This cms-htcondor-es is using "vm-legacy" branch!

```
>$ git branch
vm-legacy
```

In order to update the code base,

- Please push your commits or open your PRs against "https://github.com/dmwm/cms-htcondor-es/tree/vm-legacy".

- In virtual machine, fetch: `git fetch --all`

- Check the difference between local and remote to be sure: `git diff vm-legacy origin/vm-leagcy`

- Before rebase, please make sure if 12m period is finished:

    - No python spider_cms process: `ps aux | grep "python spider_cms.py"`

    - And `tail -f log/spider_cms.log` is like `@@@ Total processing time:...`

- Finally rebase `git pull --rebase origin vm-legacy`

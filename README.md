# recovery-tools
Tools used to backup and restore reflex in the event of catastrophic fai

## Getting Started

1. Install all dependencies using `pipenv install`
2. Create a new configuration file (e.g. `config.yml`)
3. Add the following data to the configuration file

```yaml
elasticsearch:
  hosts: ['localhost:9200']
  username: admin
  password: admin
  cacert: ''
  use_ssl: true
  tls_verifynames: false
  distro: opensearch
```

4. Run `recovery.py` in `backup` mode

```cmd
pipenv run python recovery.py -m backup -c config.yml --backup-path C:\Users\myusers\Downloads\reflex-backup
```

> :warning: **Confirm your backups succeeded before this step**

5. Delete all Reflex indices from Elasticsearch or Opensearch
6. Restart the Reflex API with `REFLEX_RECOVERY_MODE=true` environmental variable set
7. Ensure all Reflex indices are recreated
8. Stop the Reflex API
9. Run `recovery.py` in `restore` mode

> :memo: If you want to test restore to a different index use `--index-prefix test`
```cmd
pipenv run python recovery.py -m restore -c config.yml --backup-path C:\Users\myusers\Downloads\reflex-backup
```


10. Restart the Reflex API without the `REFLEX_RECOVERY_MODE` environmental variable
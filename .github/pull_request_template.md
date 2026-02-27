This PR:
*


---

#### Checklist
- The tests are passing on Github Actions (checked automatically)
- The test coverage is at least as good as before (checked automatically)
- Any model changes are reflected by migrations (checked automatically)
- [ ] If `.talismanrc` was changed, it does not contain any duplicates (each file appears at most once)
- [ ] The command `task make-dev-data` still works
- [ ] The local docker-compose dev environment still works (`task run`)
- [ ] Any new prefect flows activate Django before importing any models (`from activate_django_first import EMG_CONFIG`)
- [ ] The code style guide in `README.md` has been followed

```
gcloud auth application-default login
```

```
export TF_VAR_project_id=MYPROJECT_ID
```

```
terraform init -no-color >  init.log
```

```
terraform plan -no-color >  plan.log
```

не можна зразу зберігати лог, бо потребує погодження, під час виконання коду, ввести yes:

```
terraform apply -no-color 
```
```
terraform destroy -no-color 
```
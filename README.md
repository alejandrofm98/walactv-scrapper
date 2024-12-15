
# WALACTV-SCRAPPER

## ¿Que es?
Es un script de python que recoge los datos de eventos deportivos de la web https://www.futbolenlatv.es/
para insertarlos en firebase y mapearlos con enlaces de acestream.

## ¿Cuando se ejecuta?
El script se ejecuta todos los dias a las 8 de la mañana en un servidor propio usando un cron job y obtiene los eventos del dia actual y el siguiente (aunque se puede configurar para poner mas días).

## Como se usa
Podemos ejecutarlo usando el fichero python3 main.py, el script esta en la version 3.12 de python.
Tambien podemos usar los dos ansible playbook para configurar el servidor y que se ejecute solo con un cron job.

Deberemos tener instalado ansible en nuestra maquina local y crear el fichero maquinas basandonos en maquinas.example

Para configurar librerias del servidor
```
ansible-playbook servidores_playbook.yml -e "entorno=utilidades"
```

Para mover los recursos y claves privadas y crear el cron job en el servidor
```
ansible-playbook ansible_playbook.yml -e "entorno=utilidades"
```

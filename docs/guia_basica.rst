
Documentación LA
================



Autor: Nicolás Trejo
Tema: Learning Analytics
Madrid, Marzo 2020


Objetivo
--------

Conocer las herramientas y el entorno de investigación de LA.


edX
---

Existe una gran documentación en edX (), de la cual se recomienda leer lo siguiente:
12. Events in the Tracking Logs — EdX Research Guide documentation



https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/trac
king_logs.html
https://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/trac
king_logs/student_event_types.html


Especialmente en lo referente a los siguientes grupos de eventos

- video-interaction-events
- problem-interaction-events
- navigational-events
- discussion-forum-events
- enrollment-events
- certificate-events




Archivos de entrada de datos
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Existen diversos archivos desde los que se obtiene información del curso, los
eventos de los alumnos y su estado. A continuación veremos cada uno de ellos,
detallando su formato, utilidad e información.

Logs de Eventos
"""""""""""""""

Como su nombre indica, estos son los logs de los eventos generados por la
navegación de los alumnos (en general clicks).


Certicates
""""""""""
..
  certificate.csv

Profiles
""""""""
..
  profile.csv



"""""""""""""""


Otros
"""""

* Poll.html
* units_weight.csv
* Encuesta_accomp.csv



Python
------

Python es un lenguaje multipropósito 

Pandas
^^^^^^

.. todo::

   Write this section


Numpy
^^^^^

.. todo::

   Write this section


Sphinx
^^^^^^

Se utiliza para la documentación de este proyecto.
Guía para utilizar sphinx:
* `Python Documentation using Sphinx: <https://www.youtube.com/watch?v=MeaDUypDAoI>`_
* Primeros pasos con Sphinx: https://www.sphinx-doc.org/es/stable/tutorial.html
* A Simple Tutorial on how to document your project using Sphinx: https://medium.com/@richdayandnight/a-simple-tutorial-on-how-to-document-your-python-project-using-sphinx-and-rinohtype-177c22a15b5b
* Practical Sphinx: https://www.youtube.com/watch?v=0ROZRNZkPS8
* `CheatSheet <https://matplotlib.org/sampledoc/cheatsheet.html>`_
* `CheatSheet 2 <https://thomas-cokelaer.info/tutorials/sphinx/rest_syntax
.html>`_

* Pasos a seguir:


Desde la terminal, instalar sphinx y utilizar la herramienta interactiva (`sphinx-quickstart`) para comenzar

.. code-block:: bash

   # Instalar sphinx
   pip install sphinx

   # En la carpeta raíz del proyecto, crear la carpeta Docs
   cd <proyecto>
   mkdir Docs

   # Ejecutar el comando rápido de sphinx manteniendo opciones default
   sphinx-quickstart


Luego modificar archivo config.py (rutas, extensions y html_theme)


.. code-block:: python3

   import os
   import sys
   sys.path.insert(0, os.path.abspath('../Scripts/Python/'))
   sys.path.insert(0, os.path.abspath('../Scripts/Python/storage'))
   (...)
   extensions = [
       'sphinx.ext.autodoc',
       'sphinx.ext.todo',
   ]
   (...)
   html_theme = 'sphinx_rtd_theme'  # 'alabaster'
   (...)
   


Crear archivos rst de los módulos usando el comando de apidoc

.. code-block:: bash

   sphinx-apidoc -o Scripts/Python ../Scripts/Python/ ../Scripts/Python/bridging_mongo.original.py ../Scripts/Python/bridging_mongo_original_runable.py
   >>Creando archivo Scripts/Python/bridging_mongo.rst.
   >>El archivo Scripts/Python/bridging_mongo.rst ya existe, omitiendo.
   >>Creando archivo Scripts/Python/bridging_mongo_original_runable.rst.
   >>Creando archivo Scripts/Python/bridging_mongo_trejo_v3.rst.
   >>Creando archivo Scripts/Python/edxevent.rst.
   >>Creando archivo Scripts/Python/extracting_dates.rst.
   >>Creando archivo Scripts/Python/modules.rst.

   sphinx-apidoc -o Scripts/Python/storage ../Scripts/Python/storage/
   >>Creando archivo Scripts/Python/storage/edxmongodbstore.rst.
   >>Creando archivo Scripts/Python/storage/edxmongodbstore_trejo.rst.
   >>Creando archivo Scripts/Python/storage/modules.rst.



Modificar fichero index.rst (agregar un include para los módulos, por ejemplo)

.. code-block:: rst

   .. toctree::
      :maxdepth: 2
      :caption: Contents:

      Scripts/Python/modules
      guia_basica



.. code-block:: bash

   # Ejecutar el make
   # (puede ser necesario hacer ‘make clean’ antes para limpiar directorio `_build`)
   make clean
   make html
   # make pdf


Para usar el tema html de Read The Docs, es necesario instalarlo primero y luego agregarlo como en la variable html_theme del archivo conf.py (html_theme = 'sphinx_rtd_theme')

$    # Instalar sphinx
$    pip install sphinx


Para iniciar un servidor usando python y ver las páginas de documentación
creadas:

.. code-block:: bash

   python -m http.server


Luego abrir el navegador en `Home <http://localhost:8000/_build/html/index.html>`_


Bibliografía
------------

Edx. https://www.edx.org/es
12. Events in the Tracking Logs — EdX Research Guide documentation


Página oficial de Python. https://www.python.org/
Flask. https://flask.palletsprojects.com/en/1.1.x/
PyMongo. https://pymongo.readthedocs.io/en/stable/
Pandas
(numpy)
Sphinx https://www.sphinx-doc.org
JSON
Página oficial de MongoDB. https://www.mongodb.com/


Get up and running with MongoDB in under 5 minutes | Medium
Manage mongdo Processes | MongoDB Manual



Paso a paso

Empezamos verificando la versión de linux

$    # If necessary connect remotely using ssh:
$    # ssh user@server-name
$    # Check OS version. Other options are:
$    # lsb_release -a
$    # hostnamectl
$    cat /etc/os-release
NAME="Ubuntu"
VERSION="18.04.4 LTS (Bionic Beaver)"
ID=ubuntu
ID_LIKE=debian
PRETTY_NAME="Ubuntu 18.04.4 LTS"
VERSION_ID="18.04"
HOME_URL="https://www.ubuntu.com/"
SUPPORT_URL="https://help.ubuntu.com/"
BUG_REPORT_URL="https://bugs.launchpad.net/ubuntu/"
PRIVACY_POLICY_URL="https://www.ubuntu.com/legal/terms-and-policies/privacy-policy"
VERSION_CODENAME=bionic
UBUNTU_CODENAME=bionic



Versión del SO



Chequear si ya se cuenta con MongoDB

$    # Chequear si mongo está instalado y qué versión
$    mongo --version
No se ha encontrado la orden «mongo», pero se puede instalar con:
sudo apt install mongodb-clients

$    mongod --version
No se ha encontrado la orden «mongod», pero se puede instalar con:
sudo apt install mongodb-server-core


Chequear si mongo se encuentra instalado (en este caso no lo está)


Instalar MongoDB

$    # Actualizar listado de los paquetes
$    sudo apt update

$    # instale el propio paquete de MongoDB
$    sudo apt install mongodb

salida

El fichero se configura como se ve en la siguiente imagen:



Instalación de mongo


Paso a realizar (blanco)

$    # Comentario
$    codigo de terminal
$    codigo de terminal

$    # Comentario
$    codigo
salida

El fichero se:



Fichero modificado /etc/network/interfaces

Para que los cambios surtan efecto, es necesario:
$    # Comentario
$    codigo

Anexo II - Comandos y Trucos
ifconfig → configure a network interface
ifconfig eth0 down
ifup eth0
ip → show / manipulate routing, network devices, interfaces and tunnels
ip r → resolver los nombres imprimiendo DNS
ip a → mostrar todas las interfaces de red
vi / vim→ vim para editar ficheros de texto
ssh → OpenSSH SSH client (remote login program)
ssh <usuario>@<ip> -p [puerto]→ ejemplo “ssh root@172.17.29.3 -p 2222”
ss → another utility to investigate sockets (Sockets Statistics)
ss -tan → Muestra sockets TCP (t), todos (escuchen o no) (a) y no resuelve los nombres sino que los muestra numéricamente (n)
/etc/sysconfig/network → specifies routing and host information for all interfaces
/etc/sysconfig/network-scripts/ifcfg-ethX → fichero con la configuración de cada interface ethX (RedHat) (ver link de interés correspondiente)
openssl → OpenSSL command line tool
systemctl → Control the systemd system and service manager
systemctl status <servicio> → averiguar el estado de un servicio (por ejemplo apache2.service o virtualbox.service o mongodb.service) (creo que también se puede hacer service <servicio> status)
systemctl [disable|enable] <servicio> → habilitar o deshabilitar un servicio desde el arranque
systemctl [start|stop] <servicio> → arrancar o terminar un servicio
networkctl → Query the status of network links
Para copiar de una máquina a otra usar alguno de estos:
scp /path/to/file username@a:/path/to/destination
scp username@b:/path/to/file /path/to/destination
iptables → administration tool for IPv4 packet filtering and NAT
iptables -L -v → Lista
iptables --insert INPUT 5 --proto tcp --dport 80 --source 192.168.0.0/24 --jump ACCEPT → Se inserta una nueva regla en la cadena de INPUT (tráfico entrante) en la posición 5 (se empieza con 1) para que sea aceptada: protocolo TCP, dirigido al puerto 80 (http) y cuya IP origen corresponda a 192.168.0.1/24
ufw → Uncomplicated Firewall: program for managing a netfilter firewall
ufw enable|disable|reload → Para habilitar/deshabilitar/reiniciar el firewall en el arranque
ufw default allow|deny|reject [incoming|outgoing|routed] → 
ufw logging medium → Para que el logging sea de nivel medio (por default es ‘low’)
REGLAS:
ufw allow|deny|reject proto tcp from any to any port 80,443,8080:8090 comment 'web app' → 
# Las siguientes 3 reglas son para habilitar tráfico de las redes privadas (rfc1918)
ufw allow from 10.0.0.0/8
ufw allow from 172.16.0.0/12
ufw allow from 192.168.0.0/16






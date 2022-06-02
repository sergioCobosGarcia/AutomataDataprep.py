# lmes-pmo-automatismos-dataprep-function
Repositorio para albergar el código Python responsable de activar la Cloud Function una vez el proceso de llamada a la API de JIRA deposita los ficheros en GCS


Las Cloud Function que son disparadas via bucket de GCS tienen la limitación de 9 minutos levantadas, ya sean estas gen1 o gen2.

Este workaround fue diseñado para poder recibir los ficheros del API que tienen que ser procesados por Dataprep y lanzar de manera secuencial los jobs de dataprep que estan asociados a ese fichero.

De esta manera conseguimos evitar que se ejecuten dataprep antes de recibir el fichero correcto y mediante la recursividad conseguimos sortear la limitación de los 9 minutos, ya que cada vez que termina un Dataprep movemos un fichero dummy entre las carpetas del bucket que hace que se vuelva a llamar a la CF


Este codigo esta pensado unicamente para ser ejecutado en una CF y disparado cuando se recibe un fichero en el bucket.



# AS IS


![image-20220405-094235](https://user-images.githubusercontent.com/46417402/171605682-7764675d-14b1-458a-a3d6-638bdfb09b8b.png)



# TO BE


![image-20220525-085501](https://user-images.githubusercontent.com/46417402/171605707-4e6d810a-4b41-4797-a763-282765efa016.png)

import mysql.connector
from mysql.connector import errorcode

# Test de connexion à la base de données MySQL sur le serveur distant

try:
    cnx = mysql.connector.connect(user='remote_user',
                                  password='remote_password',
                                  host='31.38.158.71',
                                  database='Sky_Analytics')
    cursor = cnx.cursor()
    print("Connexion réussie à la base de données")
    cursor.close()
    cnx.close()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Erreur : Nom d'utilisateur ou mot de passe incorrect")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Erreur : La base de données n'existe pas")
    else:
        print(err)

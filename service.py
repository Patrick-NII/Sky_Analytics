import mysql.connector
from mysql.connector import errorcode

# Test de connexion à la base de données MySQL sur le serveur distant

try:
    cnx = mysql.connector.connect(user='Top_gun',
                                  password='zg6N&284Bb<w',
                                  host='212.227.48.180',
                                  database='Top_gun')
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

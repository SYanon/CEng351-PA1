package ceng.ceng351.cengvacdb;

import java.sql.*;
import java.util.ArrayList;


public class CENGVACDB implements ICENGVACDB {

    private static String user = "e2349819"; // TODO: Your userName
    private static String password = "8DfRhm&NW1B*"; //  TODO: Your password
    private static String host = "144.122.71.121"; // host name
    private static String database = "db2349819"; // TODO: Your database name
    private static int port = 8080; // port

    private static Connection connection = null;

    @Override
    public void initialize() {
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection =  DriverManager.getConnection(url, user, password);
        }
        catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int createTables() {
        int numberofTablesInserted = 0;
        String queryCreateUser = "create table IF NOT EXISTS User (" +
                "userID int not null," +
                "userName varchar(30)," +
                "age int," +
                "address varchar(150)," +
                "password varchar(30)," +
                "status varchar(15)," +
                "primary key (userID))";

        String queryCreateVaccine = "create table IF NOT EXISTS Vaccine  (" +
                "code int not null," +
                "vaccinename varchar(30)," +
                "type varchar(30)," +
                "primary key (code))";


        String queryCreateVaccination = "create table IF NOT EXISTS Vaccination (" +
                "code int not null," +
                "userID int not null," +
                "dose int ," +
                "vacdate date,"+
                "primary key (code,userID,dose)," +
                "foreign key (code) references Vaccine(code)," +
                "foreign key (userID) references User(userID))";


        String queryCreateAllergicSideEffect = "create table IF NOT EXISTS AllergicSideEffect (" +
                "effectcode int not null," +
                "effectname varchar(50)," +
                "primary key (effectcode))";


        String queryCreateSeen = "create table IF NOT EXISTS Seen (" +
                "effectcode int not null," +
                "code int not null," +
                "userID int not null," +
                "date date," +
                "degree varchar(30)," +
                "primary key (effectcode,code,userID),"+
                "foreign key (effectcode) references AllergicSideEffect(effectcode)" +
                "ON DELETE CASCADE, " +
                "foreign key (code) references Vaccine(code)" +
                "ON DELETE CASCADE, " +
                "foreign key (userID) references User(userID))";


        try {

            Statement statement = connection.createStatement();

            //User Table
            statement.executeUpdate(queryCreateUser);
            numberofTablesInserted++;


            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {

            Statement statement = connection.createStatement();

            //Song Table
            statement.executeUpdate(queryCreateVaccine);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {

            Statement statement = connection.createStatement();

            //artist Table
            statement.executeUpdate(queryCreateVaccination);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {

            Statement statement = connection.createStatement();

            //Album Table
            statement.executeUpdate(queryCreateAllergicSideEffect);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {

            Statement statement = connection.createStatement();

            //Listen Table
            statement.executeUpdate(queryCreateSeen);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


        return numberofTablesInserted;
    }

    @Override
    public int dropTables() {
        int numberofTablesDropped = 0;

        String queryDropUserTable = "DROP TABLE IF EXISTS User;";
        String queryDropVaccine = "DROP TABLE IF EXISTS Vaccine;";
        String queryDropVaccination = "DROP TABLE IF EXISTS Vaccination;";
        String queryDropAllergicSideEffect = "DROP TABLE IF EXISTS AllergicSideEffect;";
        String queryDropSeen = "DROP TABLE IF EXISTS Seen;";


        try {
            Statement statement = connection.createStatement();

            //User
            statement.executeUpdate(queryDropSeen);
            numberofTablesDropped++;

            //close
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = connection.createStatement();

            //Song
            statement.executeUpdate(queryDropAllergicSideEffect);
            numberofTablesDropped++;

            //close
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = connection.createStatement();

            //Artist
            statement.executeUpdate(queryDropVaccination);
            numberofTablesDropped++;

            //close
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = connection.createStatement();

            //Album
            statement.executeUpdate(queryDropVaccine);
            numberofTablesDropped++;

            //close
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = connection.createStatement();

            //Listen
            statement.executeUpdate(queryDropUserTable);
            numberofTablesDropped++;

            //close
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return numberofTablesDropped ;

    }

    @Override
    public int insertUser(User[] users) {
        int numberofRowsInserted = 0;
        int i = 0;
        while( i<users.length ) {

            String query = "insert into User values (\"" +
                    users[i].getUserID()+ "\",\"" +
                    users[i].getUserName() + "\",\"" +
                    users[i].getAge() + "\",\"" +
                    users[i].getAddress() + "\",\"" +
                    users[i].getPassword() + "\",\"" +
                    users[i].getStatus() + "\")";

            try {
                Statement st = connection.createStatement();
                st.executeUpdate(query);
                numberofRowsInserted++ ;

                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }

            i++ ;

        }
        return numberofRowsInserted;
    }

    @Override
    public int insertAllergicSideEffect(AllergicSideEffect[] sideEffects) {

        int numberofRowsInserted = 0;
        int i = 0;
        while( i<sideEffects.length ) {

            String query = "insert into AllergicSideEffect values (\"" +
                    sideEffects[i].getEffectCode()+ "\",\"" +
                    sideEffects[i].getEffectName() + "\")";

            try {
                Statement st = connection.createStatement();
                st.executeUpdate(query);
                numberofRowsInserted++ ;

                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }

            i++ ;

        }
        return numberofRowsInserted;
    }

    @Override
    public int insertVaccine(Vaccine[] vaccines) {
        int numberofRowsInserted = 0;
        int i = 0;
        while( i<vaccines.length ) {

            String query = "insert into Vaccine values (\"" +
                    vaccines[i].getCode()+ "\",\"" +
                    vaccines[i].getVaccineName() + "\",\"" +
                    vaccines[i].getType() + "\")";

            try {
                Statement st = connection.createStatement();
                st.executeUpdate(query);
                numberofRowsInserted++ ;

                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }

            i++ ;

        }
        return numberofRowsInserted;
    }

    @Override
    public int insertVaccination(Vaccination[] vaccinations) {
        int numberofRowsInserted = 0;
        int i = 0;
        while( i<vaccinations.length ) {

            String query = "insert into Vaccination values (\"" +
                    vaccinations[i].getCode()+ "\",\"" +
                    vaccinations[i].getUserID() + "\",\"" +
                    vaccinations[i].getDose() + "\",\"" +
                    vaccinations[i].getVacdate()  + "\")";

            try {
                Statement st = connection.createStatement();
                st.executeUpdate(query);
                numberofRowsInserted++ ;

                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }

            i++ ;

        }
        return numberofRowsInserted;
    }

    @Override
    public int insertSeen(Seen[] seens) {
        int numberofRowsInserted = 0;
        int i = 0;
        while( i<seens.length ) {

            String query = "insert into Seen values (\"" +
                    seens[i].getEffectcode()+ "\",\"" +
                    seens[i].getCode() + "\",\"" +
                    seens[i].getUserID() + "\",\"" +
                    seens[i].getDate() + "\",\"" +
                    seens[i].getDegree() + "\")";

            try {
                Statement st = connection.createStatement();
                st.executeUpdate(query);
                numberofRowsInserted++ ;

                //Close
                st.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }

            i++ ;

        }
        return numberofRowsInserted;
    }

    @Override
    public Vaccine[] getVaccinesNotAppliedAnyUser() {
        ArrayList<Vaccine> tmp_array = new ArrayList<>();
        ResultSet res;
        String query= "SELECT DISTINCT V1.code, V1.vaccinename, V1.type FROM Vaccine V1 " +
                "WHERE V1.code NOT IN (SELECT DISTINCT V2.code FROM Vaccination V2) " +
                "ORDER BY V1.code ASC";
        try{
            Statement statement = this.connection.createStatement();
            res = statement.executeQuery(query);

            while(res.next()){
                int code = res.getInt("code");
                String vaccinename = res.getString("vaccinename") ;
                String type = res.getString("type");
                tmp_array.add(new Vaccine(
                        code, vaccinename, type));
            }
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }

        Vaccine[] vac_array =
                new Vaccine[tmp_array.size()];

        for(int i = 0; i < tmp_array.size(); i++){
            vac_array[i] = tmp_array.get(i);
        }

        return vac_array;
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersforTwoDosesByDate(String vacdate) {
        ArrayList<QueryResult.UserIDuserNameAddressResult> tmp_array = new ArrayList<>();
        ResultSet res;

        String get_query = "SELECT DISTINCT U.userID, U.userName, U.address FROM User U WHERE U.userID IN " +
                "(SELECT V1.userID FROM Vaccination V1, Vaccination V2 "+
                "WHERE V1.userID = V2.userID AND "+
                "((V1.dose=1 AND V2.dose=2) OR (V1.dose=2 AND V2.dose=3)) AND " +
                "V1.vacdate >=\"" + vacdate + "\" "+") " +
                "ORDER BY U.userID ASC";

        try{
            Statement statement = this.connection.createStatement();
            res = statement.executeQuery(get_query);

            while(res.next()){
                int UserID = res.getInt("UserID");
                String UserID1 = "" + UserID;
                String UserName = res.getString("UserName") ;
                String address = res.getString("address");
                tmp_array.add(new QueryResult.UserIDuserNameAddressResult(
                        UserID1, UserName, address));
            }

            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }

        QueryResult.UserIDuserNameAddressResult[] user_array =
                new QueryResult.UserIDuserNameAddressResult[tmp_array.size()];

        for(int i = 0; i < tmp_array.size(); i++){
            user_array[i] = tmp_array.get(i);
        }

        return user_array;
    }



    @Override
    public Vaccine[] getTwoRecentVaccinesDoNotContainVac() {
        ArrayList<Vaccine> tmp_array = new ArrayList<>();
        ResultSet res;
        String query= "SELECT DISTINCT V1.code, V1.vaccinename, V1.type FROM Vaccine V1 " +
                "WHERE V1.vaccinename NOT LIKE '%VAC%' AND V1.code IN (SELECT V2.code FROM Vaccination V2 " +
                "WHERE V2.dose >=2) " +
                "ORDER BY V1.code ASC "+
                "LIMIT 2 ;";
        try{
            Statement statement = this.connection.createStatement();
            res = statement.executeQuery(query);

            while(res.next()){
                int code = res.getInt("code");
                String vaccinename = res.getString("vaccinename") ;
                String type = res.getString("type");
                tmp_array.add(new Vaccine(
                        code, vaccinename, type));
            }
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }

        Vaccine[] vac_array =
                new Vaccine[tmp_array.size()];

        for(int i = 0; i < tmp_array.size(); i++){
            vac_array[i] = tmp_array.get(i);
        }

        return vac_array;
    }



    @Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersAtHasLeastTwoDoseAtMostOneSideEffect() {
        ArrayList<QueryResult.UserIDuserNameAddressResult> tmp_array = new ArrayList<>();
        ResultSet res;

        String get_query = "SELECT DISTINCT U.userID, U.userName, U.address \n" +
                "FROM User U \n" +
                "WHERE U.userID IN (SELECT V1.userID \n" +
                "FROM Vaccination V1,User U \n" +
                "WHERE V1.userID= U.userID AND V1.dose >=2 AND\n" +
                "U.userID NOT IN (SELECT S1.userID \n" +
                "FROM Seen S1, Seen S2 \n" +
                "WHERE V1.userID = S1.userID AND V1.userID=S2.userID AND S1.effectcode != S2.effectcode))\n" +
                "ORDER BY U.userID ASC";

        try{
            Statement statement = this.connection.createStatement();
            res = statement.executeQuery(get_query);

            while(res.next()){
                int UserID = res.getInt("UserID");
                String UserID1 = "" + UserID;
                String UserName = res.getString("UserName") ;
                String address = res.getString("address");
                tmp_array.add(new QueryResult.UserIDuserNameAddressResult(
                        UserID1, UserName, address));
            }

            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }

        QueryResult.UserIDuserNameAddressResult[] user_array =
                new QueryResult.UserIDuserNameAddressResult[tmp_array.size()];

        for(int i = 0; i < tmp_array.size(); i++){
            user_array[i] = tmp_array.get(i);
        }

        return user_array;
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersWithAllVaccinesCanCauseGivenSideEffect(String effectname) {
        ArrayList<QueryResult.UserIDuserNameAddressResult> tmp_array = new ArrayList<>();
        ResultSet res;

        String get_query = "SELECT DISTINCT U.userID, U.userName,U.address FROM User U WHERE U.userID IN(SELECT V.userID FROM Vaccination V, Seen S, AllergicSideEffect A1 WHERE (V.code=S.code AND S.effectcode = A1.effectcode AND A1.effectname= \"" + effectname + "\"" + ")) " +
                "ORDER BY U.userID ASC;";

        try{
            Statement statement = this.connection.createStatement();
            res = statement.executeQuery(get_query);

            while(res.next()){
                int UserID = res.getInt("UserID");
                String UserID1 = "" + UserID;
                String UserName = res.getString("UserName") ;
                String address = res.getString("address");
                tmp_array.add(new QueryResult.UserIDuserNameAddressResult(
                        UserID1, UserName, address));
            }

            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }

        QueryResult.UserIDuserNameAddressResult[] user_array =
                new QueryResult.UserIDuserNameAddressResult[tmp_array.size()];

        for(int i = 0; i < tmp_array.size(); i++){
            user_array[i] = tmp_array.get(i);
        }

        return user_array;

    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersWithAtLeastTwoDifferentVaccineTypeByGivenInterval(String startdate, String enddate) {
        ArrayList<QueryResult.UserIDuserNameAddressResult> tmp_array = new ArrayList<>();
        ResultSet res;

        String get_query = "SELECT DISTINCT U.userID, U.userName,U.address FROM User U WHERE U.userID IN"+
                "(SELECT DISTINCT V1.userID FROM Vaccination V1, Vaccination V2, Vaccine VA, Vaccine VB WHERE V1.userID=V2.userID AND V1.code = VA.code AND V2.code=VB.code AND VA.type <> VB.type AND V1.vacdate >=\"" +startdate+"\"  AND V1.vacdate <=\""+enddate+"\" AND V2.vacdate >=\"" +startdate+ "\" AND V2.vacdate <=\""+enddate+ "\" )"+
                "ORDER by U.userID ASC;";

        try{
            Statement statement = this.connection.createStatement();
            res = statement.executeQuery(get_query);

            while(res.next()){
                int UserID = res.getInt("UserID");
                String UserID1 = "" + UserID;
                String UserName = res.getString("UserName") ;
                String address = res.getString("address");
                tmp_array.add(new QueryResult.UserIDuserNameAddressResult(
                        UserID1, UserName, address));
            }

            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }

        QueryResult.UserIDuserNameAddressResult[] user_array =
                new QueryResult.UserIDuserNameAddressResult[tmp_array.size()];

        for(int i = 0; i < tmp_array.size(); i++){
            user_array[i] = tmp_array.get(i);
        }

        return user_array;
    }

    @Override
    public AllergicSideEffect[] getSideEffectsOfUserWhoHaveTwoDosesInLessThanTwentyDays() {
        ArrayList<AllergicSideEffect> tmp_array = new ArrayList<>();
        ResultSet res;
        String query= "SELECT DISTINCT A.effectcode,A.effectname FROM Vaccination V1, Vaccination V2 ,Seen S,AllergicSideEffect A WHERE V1.userID=V2.userID AND V1.dose=2 AND V2.dose=1 AND (DATEDIFF(V1.vacdate, V2.vacdate) < 20) AND V1.UserID=S.userID AND S.effectcode=A.effectcode ORDER BY A.effectcode ASC;";
        try{
            Statement statement = this.connection.createStatement();
            res = statement.executeQuery(query);

            while(res.next()){
                int effectcode = res.getInt("effectcode");
                String effectename = res.getString("effectname") ;
                tmp_array.add(new AllergicSideEffect(
                        effectcode,effectename));
            }
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }

        AllergicSideEffect[] vac_array =
                new AllergicSideEffect[tmp_array.size()];

        for(int i = 0; i < tmp_array.size(); i++){
            vac_array[i] = tmp_array.get(i);
        }

        return vac_array;
    }


    @Override
    public double averageNumberofDosesofVaccinatedUserOverSixtyFiveYearsOld() {
        double avg=0;
        ResultSet res;

        String query = "SELECT AVG(Result.subres)\n" +
                "FROM User U5, (SELECT U.userID,MAX(V2.dose) AS subres\n" +
                "FROM User U, Vaccination V1, Vaccination V2 \n" +
                "WHERE (U.age >65 AND V2.dose>=V1.dose AND V1.userID=U.UserID AND V2.userID=U.UserID  AND V2.UserID=V1.UserID)\n" +
                "GROUP BY U.userID) Result";
        try{
            Statement statement = this.connection.createStatement();
            res = statement.executeQuery(query);

            while(res.next()) {
                avg=res.getDouble(1);
            }
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return avg;
    }

    @Override
    public int updateStatusToEligible(String givendate) {
        String update_query = "UPDATE User U,(SELECT Result.userID\n" +
                "        FROM User U5,(SELECT U.userID,MAX(V2.dose) AS subres\n" +
                "        FROM User U, Vaccination V1, Vaccination V2\n" +
                "        WHERE (V2.dose>V1.dose AND DATEDIFF(\"2021-12-19\",V2.vacdate)>=120 AND V1.userID=U.UserID AND V2.userID=U.UserID  AND V2.UserID=V1.UserID AND U.status='Not_Eligible')\n" +
                "        GROUP BY U.userID) Result\n" +
                "        WHERE U5.UserID=Result.userID) subres2\n" +
                "        SET U.status = 'Eligible'\n" +
                "        WHERE U.userID=subres2.UserID;";

        int number_of_updates = 0;

        try{
            Statement statement = this.connection.createStatement();
            number_of_updates = statement.executeUpdate(update_query);

            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }

        return number_of_updates;

    }


    @Override
    public Vaccine deleteVaccine(String vaccineName) {

        Vaccine r = null ;
        ResultSet rs ;

        String get_query = "SELECT *\n" +
                "FROM Vaccine V\n" +
                "WHERE V.vaccinename=\"Convidecia\"";

        try {
            Statement st = connection.createStatement();
            rs = st.executeQuery(get_query);

            rs.next();

            int r_code= rs.getInt("code");
            String r_vaccinename = rs.getString("vaccinename");
            String r_type = rs.getString("type");

            r = new Vaccine(r_code, r_vaccinename, r_type);

            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        String delete_query = "DELETE FROM Vaccine " +
                "WHERE vaccineName = \"" + vaccineName + "\" ";

        try{
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(delete_query);
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return r;

    }
}
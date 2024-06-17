package top.soaringlab.test.weather.oj;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * @author rillusory
 * @Description
 * @date 4/21/24 11:23 PM
 **/
public class TemperatureLog {
    private static final String[] cities = {"Vancouver", "Portland", "San Francisco", "Seattle", "Los Angeles",
            "San Diego", "Las Vegas", "Phoenix", "Albuquerque", "Denver", "San Antonio",
            "Dallas", "Houston", "Kansas City", "Minneapolis", "Saint Louis", "Chicago",
            "Nashville", "Indianapolis", "Atlanta", "Detroit", "Jacksonville", "Charlotte",
            "Miami", "Pittsburgh", "Toronto", "Philadelphia", "New York", "Montreal", "Boston",
            "Beersheba", "Tel Aviv District", "Eilat", "Haifa", "Nahariyya", "Jerusalem"};
    private String datetime;
    private double Vancouver;
    private double Portland;
    private double SanFrancisco;
    private double Seattle;
    private double LosAngeles;
    private double SanDiego;
    private double LasVegas;
    private double Phoenix;
    private double Albuquerque;
    private double Denver;
    private double SanAntonio;
    private double Dallas;
    private double Houston;
    private double KansasCity;
    private double Minneapolis;
    private double SaintLouis;
    private double Chicago;
    private double Nashville;
    private double Indianapolis;
    private double Atlanta;
    private double Detroit;
    private double Jacksonville;
    private double Charlotte;
    private double Miami;
    private double Pittsburgh;
    private double Toronto;
    private double Philadelphia;
    private double NewYork;
    private double Montreal;
    private double Boston;
    private double Beersheba;
    private double TelAvivDistrict;
    private double Eilat;
    private double Haifa;
    private double Nahariyya;
    private double Jerusalem;


    public TemperatureLog(String datetime, double vancouver, double portland, double sanFrancisco, double seattle,
                          double losAngeles, double sanDiego, double lasVegas, double phoenix, double albuquerque,
                          double denver, double sanAntonio, double dallas, double houston, double kansasCity,
                          double minneapolis, double saintLouis, double chicago, double nashville, double indianapolis,
                          double atlanta, double detroit, double jacksonville, double charlotte, double miami, double pittsburgh,
                          double toronto, double philadelphia, double newYork, double montreal, double boston, double beersheba,
                          double telAvivDistrict, double eilat, double haifa, double nahariyya, double jerusalem) {
        this.datetime = datetime;
        Vancouver = vancouver;
        Portland = portland;
        SanFrancisco = sanFrancisco;
        Seattle = seattle;
        LosAngeles = losAngeles;
        SanDiego = sanDiego;
        LasVegas = lasVegas;
        Phoenix = phoenix;
        Albuquerque = albuquerque;
        Denver = denver;
        SanAntonio = sanAntonio;
        Dallas = dallas;
        Houston = houston;
        KansasCity = kansasCity;
        Minneapolis = minneapolis;
        SaintLouis = saintLouis;
        Chicago = chicago;
        Nashville = nashville;
        Indianapolis = indianapolis;
        Atlanta = atlanta;
        Detroit = detroit;
        Jacksonville = jacksonville;
        Charlotte = charlotte;
        Miami = miami;
        Pittsburgh = pittsburgh;
        Toronto = toronto;
        Philadelphia = philadelphia;
        NewYork = newYork;
        Montreal = montreal;
        Boston = boston;
        Beersheba = beersheba;
        TelAvivDistrict = telAvivDistrict;
        Eilat = eilat;
        Haifa = haifa;
        Nahariyya = nahariyya;
        Jerusalem = jerusalem;
    }

    public void setVancouver(double vancouver) {
        Vancouver = vancouver;
    }

    public double getPortland() {
        return Portland;
    }

    public void setPortland(double portland) {
        Portland = portland;
    }

    public double getSanFrancisco() {
        return SanFrancisco;
    }

    public void setSanFrancisco(double sanFrancisco) {
        SanFrancisco = sanFrancisco;
    }

    public double getSeattle() {
        return Seattle;
    }

    public void setSeattle(double seattle) {
        Seattle = seattle;
    }

    public double getLosAngeles() {
        return LosAngeles;
    }

    public void setLosAngeles(double losAngeles) {
        LosAngeles = losAngeles;
    }

    public double getSanDiego() {
        return SanDiego;
    }

    public void setSanDiego(double sanDiego) {
        SanDiego = sanDiego;
    }

    public double getLasVegas() {
        return LasVegas;
    }

    public void setLasVegas(double lasVegas) {
        LasVegas = lasVegas;
    }

    public double getPhoenix() {
        return Phoenix;
    }

    public void setPhoenix(double phoenix) {
        Phoenix = phoenix;
    }

    public double getAlbuquerque() {
        return Albuquerque;
    }

    public void setAlbuquerque(double albuquerque) {
        Albuquerque = albuquerque;
    }

    public double getDenver() {
        return Denver;
    }

    public void setDenver(double denver) {
        Denver = denver;
    }

    public double getSanAntonio() {
        return SanAntonio;
    }

    public void setSanAntonio(double sanAntonio) {
        SanAntonio = sanAntonio;
    }

    public double getDallas() {
        return Dallas;
    }

    public void setDallas(double dallas) {
        Dallas = dallas;
    }

    public double getHouston() {
        return Houston;
    }

    public void setHouston(double houston) {
        Houston = houston;
    }

    public double getKansasCity() {
        return KansasCity;
    }

    public void setKansasCity(double kansasCity) {
        KansasCity = kansasCity;
    }

    public double getMinneapolis() {
        return Minneapolis;
    }

    public void setMinneapolis(double minneapolis) {
        Minneapolis = minneapolis;
    }

    public double getSaintLouis() {
        return SaintLouis;
    }

    public void setSaintLouis(double saintLouis) {
        SaintLouis = saintLouis;
    }

    public double getChicago() {
        return Chicago;
    }

    public void setChicago(double chicago) {
        Chicago = chicago;
    }

    public double getNashville() {
        return Nashville;
    }

    public void setNashville(double nashville) {
        Nashville = nashville;
    }

    public double getIndianapolis() {
        return Indianapolis;
    }

    public void setIndianapolis(double indianapolis) {
        Indianapolis = indianapolis;
    }

    public double getAtlanta() {
        return Atlanta;
    }

    public void setAtlanta(double atlanta) {
        Atlanta = atlanta;
    }

    public double getDetroit() {
        return Detroit;
    }

    public void setDetroit(double detroit) {
        Detroit = detroit;
    }

    public double getJacksonville() {
        return Jacksonville;
    }

    public void setJacksonville(double jacksonville) {
        Jacksonville = jacksonville;
    }

    public double getCharlotte() {
        return Charlotte;
    }

    public void setCharlotte(double charlotte) {
        Charlotte = charlotte;
    }

    public double getMiami() {
        return Miami;
    }

    public void setMiami(double miami) {
        Miami = miami;
    }

    public double getPittsburgh() {
        return Pittsburgh;
    }

    public void setPittsburgh(double pittsburgh) {
        Pittsburgh = pittsburgh;
    }

    public double getToronto() {
        return Toronto;
    }

    public void setToronto(double toronto) {
        Toronto = toronto;
    }

    public double getPhiladelphia() {
        return Philadelphia;
    }

    public void setPhiladelphia(double philadelphia) {
        Philadelphia = philadelphia;
    }

    public double getNewYork() {
        return NewYork;
    }

    public void setNewYork(double newYork) {
        NewYork = newYork;
    }

    public double getMontreal() {
        return Montreal;
    }

    public void setMontreal(double montreal) {
        Montreal = montreal;
    }

    public double getBoston() {
        return Boston;
    }

    public void setBoston(double boston) {
        Boston = boston;
    }

    public double getBeersheba() {
        return Beersheba;
    }

    public void setBeersheba(double beersheba) {
        Beersheba = beersheba;
    }

    public double getTelAvivDistrict() {
        return TelAvivDistrict;
    }

    public void setTelAvivDistrict(double telAvivDistrict) {
        TelAvivDistrict = telAvivDistrict;
    }

    public double getEilat() {
        return Eilat;
    }

    public void setEilat(double eilat) {
        Eilat = eilat;
    }

    public double getHaifa() {
        return Haifa;
    }

    public void setHaifa(double haifa) {
        Haifa = haifa;
    }

    public double getNahariyya() {
        return Nahariyya;
    }

    public void setNahariyya(double nahariyya) {
        Nahariyya = nahariyya;
    }

    public double getJerusalem() {
        return Jerusalem;
    }

    public void setJerusalem(double jerusalem) {
        Jerusalem = jerusalem;
    }

    // Getters and Setters
    public String getDatetime() {
        return datetime;
    }

    public void setDatetime(String datetime) {
        this.datetime = datetime;
    }

    public double getVancouver() {
        return Vancouver;
    }

    public double getTemperatureByName(String cityName) throws NoSuchFieldException, IllegalAccessException {
        Field field = this.getClass().getDeclaredField(cityName.replace(" ", ""));

        return field.getDouble(this);
    }

    public String[] getCities() {
        return cities;
    }

    @Override
    public String toString() {
        return "TemperatureLog{" +
                "datetime='" + datetime + '\'' +
                ", Vancouver=" + Vancouver +
                ", Portland=" + Portland +
                ", SanFrancisco=" + SanFrancisco +
                ", Seattle=" + Seattle +
                ", LosAngeles=" + LosAngeles +
                ", SanDiego=" + SanDiego +
                ", LasVegas=" + LasVegas +
                ", Phoenix=" + Phoenix +
                ", Albuquerque=" + Albuquerque +
                ", Denver=" + Denver +
                ", SanAntonio=" + SanAntonio +
                ", Dallas=" + Dallas +
                ", Houston=" + Houston +
                ", KansasCity=" + KansasCity +
                ", Minneapolis=" + Minneapolis +
                ", SaintLouis=" + SaintLouis +
                ", Chicago=" + Chicago +
                ", Nashville=" + Nashville +
                ", Indianapolis=" + Indianapolis +
                ", Atlanta=" + Atlanta +
                ", Detroit=" + Detroit +
                ", Jacksonville=" + Jacksonville +
                ", Charlotte=" + Charlotte +
                ", Miami=" + Miami +
                ", Pittsburgh=" + Pittsburgh +
                ", Toronto=" + Toronto +
                ", Philadelphia=" + Philadelphia +
                ", NewYork=" + NewYork +
                ", Montreal=" + Montreal +
                ", Boston=" + Boston +
                ", Beersheba=" + Beersheba +
                ", TelAvivDistrict=" + TelAvivDistrict +
                ", Eilat=" + Eilat +
                ", Haifa=" + Haifa +
                ", Nahariyya=" + Nahariyya +
                ", Jerusalem=" + Jerusalem +
                '}';
    }



}


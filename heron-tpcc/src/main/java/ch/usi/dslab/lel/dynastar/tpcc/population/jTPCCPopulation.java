/*
 * jTPCCPopulation - Tpc-c population 
 *
 * Copyright (C) 2005, Mohamed Tlemsani <mtlemsani@users.sourceforge.net>
 *
 * jTPCCPopulation is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package ch.usi.dslab.lel.dynastar.tpcc.population;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;
/**  
 * @author <a href="mailto:mtlemsani@users.sourceforge.net">Mohamed Tlemsani</a>
 * @version $Id: jTPCCPopulation.java,v 1.3 2005/07/12 14:29:19 mtl Exp $
 * 
 * Automatic generator of TPCC data, allow to populate database 
 * with generic data according to the Tpcc specifications 
// * @see chapter 4 (SCALING AND DATABASE POPULATION)
 */
public class jTPCCPopulation {
    
    private final static Logger _log = Logger.getLogger(jTPCCPopulation.class);
    private long POP_C_LAST = jTPCCTools.NULL_NUMBER;
    private long POP_C_ID = jTPCCTools.NULL_NUMBER;
    private long POP_OL_I_ID = jTPCCTools.NULL_NUMBER;
    
    private int _seqIdCustomer[];
    private boolean _new_order = false;
    private Connection _conn = null;
    private String _dburl = "";
    private String _userdb = "";
    private String _pwddb = "";
    private String _host = "";
    private String _port = "";
    private String _db = "";
    private String _connector = "";
    private String _product = "";
    private String _driver = "";
    private Properties _prop = null;
    private PreparedStatement _pstmt = null;

    private int _nb_warehouse=1;
    
    private jTPCCPopulation() {
        init();
        makeConnection();
    }

    
    public jTPCCPopulation(int nb_wharehouse) {
        this();
        _nb_warehouse = nb_wharehouse;
        populate_item();        
        populate_wharehouse();
        storeProperties();
        close_database();
    }
    
    
    /*
     * initialise les parametres de la base puis ouvre une connection  
     */
    private void init() {
        this._seqIdCustomer = new int[jTPCCTools.NB_MAX_CUSTOMER];
        try {
            String afile = System.getProperty("user.dir") + File.separator
            + System.getProperty("tpcc.file");
            _log.debug("init file " + afile) ;
            
            FileInputStream fis = new FileInputStream(afile);
            this._prop = new Properties();
            this._prop.load(fis);            
            
            this._product = System.getProperty("product");
            String akey = "database." + this._product + ".";
            
            this._userdb = this._prop.getProperty(akey + "user");
            this._pwddb = this._prop.getProperty(akey + "pwd");
            this._host = this._prop.getProperty(akey + "host");
            this._port = this._prop.getProperty(akey + "port");
            this._connector = this._prop.getProperty(akey + "connector");
            this._db = this._prop.getProperty(akey + "db");
            this._driver = this._prop.getProperty(akey + "driver");
            if (this._prop.getProperty(akey + "url") != null ) {
                this._dburl = this._prop.getProperty(akey + "url");		    
            } else {
                this._dburl = this._connector + ":" + this._product + "://"
                + this._host + ":" + this._port + "/" + this._db;
            }    
        } catch (IOException e) {
            _log.error("IOException : " + e.toString());
        } catch (java.lang.IllegalArgumentException e) {
            _log.error("IllegalArgumentException : " + e.toString());
        } catch (java.lang.NullPointerException e) {
            e.printStackTrace();
            _log.error("NullPointerException : " + e.getMessage());
        }
    }
    
    private void makeConnection() {
        try {
            _log.debug("Tentative connexion Base "+this._dburl);
            Class.forName(this._driver).newInstance();
            this._conn = DriverManager.getConnection(this._dburl, this._userdb, this._pwddb);
            _log.debug("Connexion Base "+this._dburl+" reussi");
            this._conn.setAutoCommit(false);
        } catch (ClassNotFoundException e) {
            _log.error("ClassNotFoundException "+e.toString());
        } catch (IllegalAccessException e) {
            _log.error("IllegalAccessException : " + e.toString());
        } catch (InstantiationException e) {
            _log.error("InstantiationException : " + e.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            _log.error("SQLException : " + e.getMessage());
            System.exit(1);
        } catch (java.lang.IllegalArgumentException e) {
            _log.error("IllegalArgumentException : " + e.toString());
        } catch (java.lang.NullPointerException e) {
            e.printStackTrace();
            _log.error("NullPointerException : " + e.getMessage());
            System.exit(1);
        } catch (ExceptionInInitializerError e) {
            _log.error("ExceptionInInitializerError : " + e.toString());
        } catch (java.lang.SecurityException e) {
            _log.error("SecurityException : " + e.toString());
        }        
    }

    
    private void storeProperties() {        
        try {
            String filename= System.getProperty("user.dir") + File.separator;
            filename += "sql" + File.separator + this._product + File.separator ;
            filename += this._product + "_" + _nb_warehouse + ".properties";
            File file = new File(filename);
            FileOutputStream aFile = new FileOutputStream(file, false);
            System.out.println("File "+filename);
            Properties prop = new Properties();
            prop.setProperty("C_LAST", String.valueOf(getC_LAST()));
            prop.setProperty("C_ID", String.valueOf(getC_ID()));
            prop.setProperty("OL_I_ID", String.valueOf(getOL_I_ID()));
            String comments  = " Automatic generation by Tpcc Population \n";
            comments += "# Tpcc Population for "+_nb_warehouse+" warehouse in "+this._product;
            comments += " product\n"+"# Tpcc rev. 5.4 - Chapitre 2.1.6.1"; 
            prop.store(aFile, comments);
            aFile.close();
        } catch (FileNotFoundException e) {
            _log.error("FileNotFoundException : " + e.toString());
        } catch (IOException e) {
            _log.error("IOException : " + e.toString());
        } finally {
        }
    }
    
    
    private void populate_wharehouse() {
        if (_nb_warehouse > 0) {
            for (int i = 1; i <= _nb_warehouse; i++) {
                _log.info(" WAREHOUSE " + i);
                String requete = "INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, ";
                requete += "w_city, w_state, w_zip, w_tax, w_ytd) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                
                try {
                    this._pstmt = this._conn.prepareStatement(requete);
                    this._pstmt.setInt(1, i);
                    this._pstmt.setString(2, jTPCCTools.alea_chainec(6, 10));
                    this._pstmt.setString(3, jTPCCTools.alea_chainec(10, 20));
                    this._pstmt.setString(4, jTPCCTools.alea_chainec(10, 20));
                    this._pstmt.setString(5, jTPCCTools.alea_chainec(10, 20));
                    this._pstmt.setString(6, jTPCCTools.alea_chainel(2, 2));
                    this._pstmt.setString(7, jTPCCTools.alea_chainen(4, 4) + jTPCCTools.CHAINE_5_1);
                    this._pstmt.setFloat(8, jTPCCTools.alea_float(Float.valueOf("0.0000")
                            .floatValue(), Float.valueOf("0.2000").floatValue(), 4));
                    this._pstmt.setDouble(9, jTPCCTools.WAREHOUSE_YTD);
                    int res = this._pstmt.executeUpdate();
                    if (res != 1) _log.warn("requete non effectue " + requete);
                    this._conn.commit();
                } catch (SQLException e) {
                    manage_exception(e);
                } catch (Throwable e) {
                    manage_exception(e);
                } finally {
                    close_statement();
                }
                populate_stock(i);
                populate_district(i);
            }
        }
    }
    
    
    
    private void populate_district(int id_wharehouse) {
        if (id_wharehouse < 0) return;
        else {
            for (int id_district = 1; id_district <= jTPCCTools.NB_MAX_DISTRICT; id_district++) {
                _log.info(" DISTRICT " + id_district);
                String requete = "INSERT INTO district (d_id, d_w_id, d_name, d_street_1, d_street_2, ";
                requete += "d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                
                try {
                    this._pstmt = this._conn.prepareStatement(requete);
                    this._pstmt.setInt(1, id_district);
                    this._pstmt.setInt(2, id_wharehouse);
                    this._pstmt.setString(3, jTPCCTools.alea_chainec(6, 10));
                    this._pstmt.setString(4, jTPCCTools.alea_chainec(10, 20));
                    this._pstmt.setString(5, jTPCCTools.alea_chainec(10, 20));
                    this._pstmt.setString(6, jTPCCTools.alea_chainec(10, 20));
                    this._pstmt.setString(7, jTPCCTools.alea_chainel(2, 2));
                    this._pstmt.setString(8, jTPCCTools.alea_chainen(4, 4) + jTPCCTools.CHAINE_5_1);
                    this._pstmt.setFloat(9, jTPCCTools.alea_float(Float.valueOf("0.0000")
                            .floatValue(), Float.valueOf("0.2000").floatValue(), 4));
                    this._pstmt.setDouble(10, jTPCCTools.WAREHOUSE_YTD);
                    this._pstmt.setInt(11, 3001);
                    int res = this._pstmt.executeUpdate();
                    if (res != 1) _log.warn("requete non effectue " + requete);
                    this._conn.commit();
                } catch (SQLException e) {
                    manage_exception(e);
                } catch (Throwable e) {
                    manage_exception(e);
                } finally {
                    close_statement();
                }
                populate_customer(id_wharehouse, id_district);
                populate_order(id_wharehouse, id_district);
            }
        }
    }
    
    private void populate_order(int id_wharehouse, int id_district) {
        this._new_order = false;
        _log.info(" ORDER "+id_wharehouse+", "+id_district );
        for (int id_order = 1; id_order <= jTPCCTools.NB_MAX_ORDER; id_order++) {
            String requete = "INSERT INTO orderr (o_id, o_w_id, o_d_id, o_ol_cnt, o_all_local, ";
            requete += "o_c_id, o_entry_d, o_carrier_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            int o_ol_cnt = jTPCCTools.alea_number(5, 15);
            Date aDate = new Date((new java.util.Date()).getTime());
            try {
                this._pstmt = this._conn.prepareStatement(requete);
                this._pstmt.setInt(1, id_order);
                this._pstmt.setInt(2, id_wharehouse);
                this._pstmt.setInt(3, id_district);
                this._pstmt.setInt(4, o_ol_cnt);
                this._pstmt.setInt(5, 1);
                this._pstmt.setInt(6, generate_seq_alea(0, jTPCCTools.NB_MAX_CUSTOMER-1));
                this._pstmt.setDate(7, aDate);
                if (id_order < jTPCCTools.LIMIT_ORDER) this._pstmt.setInt(8, jTPCCTools.alea_number(1, 10));
                else this._pstmt.setInt(8, 0);
                int res = this._pstmt.executeUpdate();
                if (res != 1) _log.warn("requete non effectue " + requete);
                this._conn.commit();
            } catch (SQLException e) {
                manage_exception(e);
            } catch (Throwable e) {
                manage_exception(e);
            } finally {
                close_statement();
            }
            populate_order_line(id_wharehouse, id_district, id_order, o_ol_cnt, aDate);
            if (id_order >= jTPCCTools.LIMIT_ORDER) populate_new_order(id_wharehouse, id_district, id_order);
        }
    }
    
    // Genere n (o_ol_cnt) order_line
    private void populate_order_line(int id_wharehouse, int id_district,
            int id_order, int o_ol_cnt, Date aDate) {
        for (int i = 0; i < o_ol_cnt; i++) {
            String requete = "INSERT INTO order_line (ol_o_id, ol_w_id, ol_d_id, ol_number, ol_amount, ";
            requete += "ol_delivery_d, ol_dist_info, ol_i_id, ol_quantity, ol_supply_w_id) ";
            requete += "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            try {
                this._pstmt = this._conn.prepareStatement(requete);
                this._pstmt.setInt(1, id_order);
                this._pstmt.setInt(2, id_wharehouse);
                this._pstmt.setInt(3, id_district);
                this._pstmt.setInt(4, i);
                if (id_order >= jTPCCTools.LIMIT_ORDER) {
                    this._pstmt.setFloat(5, (float) jTPCCTools.alea_double(0.01, 9999.99, 2));
                    this._pstmt.setDate(6, null);
                }
                else { 
                    this._pstmt.setFloat(5, 0);
                    this._pstmt.setDate(6, aDate); // Afaire pour chaque type de base (sysdate, todate etcc...)
                }
                this._pstmt.setString(7, jTPCCTools.alea_chainel(12, 24));
                this._pstmt.setLong(8, jTPCCTools.nonUniformRandom(getOL_I_ID(), jTPCCTools.A_OL_I_ID, 1L, jTPCCTools.NB_MAX_ITEM));
                this._pstmt.setInt(9, 5);
                this._pstmt.setInt(10, id_wharehouse);
                int res = this._pstmt.executeUpdate();
                if (res != 1) _log.warn("requete non effectue " + requete);
                this._conn.commit();
            } catch (SQLException e) {
                manage_exception(e);
            } catch (Throwable e) {
                manage_exception(e);
            } finally {
                close_statement();
            }
        }
    }
    
    
    private void populate_new_order(int id_wharehouse, int id_district, int id_order) {
        String requete = "INSERT INTO new_order (no_d_id, no_o_id, no_w_id) ";
        requete += "VALUES (?, ?, ?)";
        
        try {
            this._pstmt = this._conn.prepareStatement(requete);
            this._pstmt.setInt(1, id_district);
            this._pstmt.setInt(2, id_order);
            this._pstmt.setInt(3, id_wharehouse);
            int res = this._pstmt.executeUpdate();
            if (res != 1) _log.warn("requete non effectue " + requete);
            this._conn.commit();
        } catch (SQLException e) {
            manage_exception(e);
        } catch (Throwable e) {
            manage_exception(e);
        } finally {
            close_statement();
        }
    }
    
    
    private void populate_item() {
        _log.info(" ITEM ");
        for (long i = 1; i <= jTPCCTools.NB_MAX_ITEM; i++) {
            String requete = "INSERT INTO item (i_id, i_name, i_price, i_data, i_im_id) ";
            requete += "VALUES (?, ?, ?, ?, ?)";
            
            try {
                this._pstmt = this._conn.prepareStatement(requete);
                this._pstmt.setLong(1, i);
                this._pstmt.setString(2, jTPCCTools.alea_chainec(14, 24));
                this._pstmt.setFloat(3, jTPCCTools.alea_float(1, 100, 2));
                this._pstmt.setString(4, jTPCCTools.s_data());
                this._pstmt.setInt(5, jTPCCTools.alea_number(1, 10000));
                int res = this._pstmt.executeUpdate();
                if (res != 1) _log.warn("requete non effectue " + requete);
                this._conn.commit();
            } catch (SQLException e) {
                manage_exception(e);
            } catch (Throwable e) {
                manage_exception(e);
            } finally {
                close_statement();
            }
        }
    }
    
    private void populate_stock(int id_wharehouse) {
        if (id_wharehouse < 0) return;
        else {
            _log.info(" STOCK for Warehouse "+id_wharehouse);
            for (long i = 1; i <= jTPCCTools.NB_MAX_ITEM; i++) {
                String requete = "INSERT INTO stock (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02,";
                requete += " s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd,";
                requete += " s_order_cnt, s_remote_cnt, s_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                
                try {
                    this._pstmt = this._conn.prepareStatement(requete);
                    this._pstmt.setLong(1, i);
                    this._pstmt.setInt(2, id_wharehouse);
                    this._pstmt.setInt(3, jTPCCTools.alea_number(10, 100));
                    this._pstmt.setString(4, jTPCCTools.alea_chainel(24, 24));
                    this._pstmt.setString(5, jTPCCTools.alea_chainel(24, 24));
                    this._pstmt.setString(6, jTPCCTools.alea_chainel(24, 24));
                    this._pstmt.setString(7, jTPCCTools.alea_chainel(24, 24));
                    this._pstmt.setString(8, jTPCCTools.alea_chainel(24, 24));
                    this._pstmt.setString(9, jTPCCTools.alea_chainel(24, 24));
                    this._pstmt.setString(10, jTPCCTools.alea_chainel(24, 24));
                    this._pstmt.setString(11, jTPCCTools.alea_chainel(24, 24));
                    this._pstmt.setString(12, jTPCCTools.alea_chainel(24, 24));
                    this._pstmt.setString(13, jTPCCTools.alea_chainel(24, 24));
                    this._pstmt.setDouble(14, 0);
                    this._pstmt.setInt(15, 0);
                    this._pstmt.setInt(16, 0);
                    this._pstmt.setString(17, jTPCCTools.s_data());
                    int res = this._pstmt.executeUpdate();
                    if (res != 1) _log.warn("requete non effectue " + requete);
                    this._conn.commit();
                } catch (SQLException e) {
                    manage_exception(e);
                } catch (Throwable e) {
                    manage_exception(e);
                } finally {
                    close_statement();
                }
            }
        }
    }
    
    private void populate_customer(int id_wharehouse, int id_district) {
        if (id_wharehouse < 0 || id_district < 0) return;
        else {
            _log.info(" CUSTOMER "+id_wharehouse+", "+id_district );
            for (int i = 1; i <= jTPCCTools.NB_MAX_CUSTOMER; i++) {
                String requete = "INSERT INTO customer (c_id, c_d_id, c_w_id, c_first, c_middle, ";
                requete += "c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, ";
                requete += "c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, ";
                requete += "c_delivery_cnt, c_data) ";
                requete += "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                
                try {
                    this._pstmt = this._conn.prepareStatement(requete);
                    this._pstmt.setInt(1, i);
                    this._pstmt.setInt(2, id_district);
                    this._pstmt.setInt(3, id_wharehouse);
                    this._pstmt.setString(4, jTPCCTools.alea_chainec(8, 16));
                    this._pstmt.setString(5, "OE");
                    this._pstmt.setString(6, c_last());
                    this._pstmt.setString(7, jTPCCTools.alea_chainec(10, 20));
                    this._pstmt.setString(8, jTPCCTools.alea_chainec(10, 20));
                    this._pstmt.setString(9, jTPCCTools.alea_chainec(10, 20));
                    this._pstmt.setString(10, jTPCCTools.alea_chainel(2, 2));
                    this._pstmt.setString(11, jTPCCTools.alea_chainen(4, 4) + jTPCCTools.CHAINE_5_1);
                    this._pstmt.setString(12, jTPCCTools.alea_chainen(16, 16));
                    this._pstmt.setDate(13, new Date(System.currentTimeMillis()));
                    this._pstmt.setString(14, (jTPCCTools.alea_number(1, 10) == 1) ? "BC" : "GC");
                    this._pstmt.setDouble(15, 500000);
                    this._pstmt.setFloat(16, (float) jTPCCTools.alea_double(0., 0.5, 4));
                    this._pstmt.setDouble(17, -10.0);
                    this._pstmt.setDouble(18, 10.0);
                    this._pstmt.setInt(19, 1);
                    this._pstmt.setInt(20, 0);
                    this._pstmt.setString(21, jTPCCTools.alea_chainec(300, 500));
                    int res = this._pstmt.executeUpdate();
                    if (res != 1) _log.warn("requete non effectue " + requete);
                    this._conn.commit();
                } catch (SQLException e) {
                    manage_exception(e);
                } catch (Throwable e) {
                    manage_exception(e);
                } finally {
                    close_statement();
                }
                populate_history(i, id_wharehouse, id_district);
            }
        }
    }
    
    private void populate_history(int id_customer, int id_wharehouse, int id_district) {
        if (id_customer < 0 || id_wharehouse < 0 || id_district < 0) return;
        else {            
            String requete = "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, ";
            requete += "h_date, h_amount, h_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            
            try {
                this._pstmt = this._conn.prepareStatement(requete);
                this._pstmt.setInt(1, id_customer);
                this._pstmt.setInt(2, id_district);
                this._pstmt.setInt(3, id_wharehouse);
                this._pstmt.setInt(4, id_district);
                this._pstmt.setInt(5, id_wharehouse);
                this._pstmt.setDate(6, new Date(System.currentTimeMillis()));
                this._pstmt.setFloat(7, 10);
                this._pstmt.setString(8, jTPCCTools.alea_chainec(12, 24));
                int res = this._pstmt.executeUpdate();
                if (res != 1) _log.warn("requete non effectue " + requete);
                this._conn.commit();
            } catch (SQLException e) {
                manage_exception(e);
            } catch (Throwable e) {
                manage_exception(e);
            } finally {
                close_statement();
            }
        }
    }
    
    
    
    private void manage_exception(Throwable e) {
        try {
            this._conn.rollback();
        } catch (SQLException sql) {
            sql.printStackTrace();
            _log.error("Rollback error:	" + sql.getMessage());
        } finally {
            try {
                this._conn.close();
            } catch (SQLException sql) {
                sql.printStackTrace();
                _log.error("DB Close error:	" + sql.getMessage());
            }
        }
        e.printStackTrace();
        _log.error("Query.connect: " + e.getMessage());
    }
    
    private void close_statement() {
        try {
            this._pstmt.close();
        } catch (SQLException sql) {
            sql.printStackTrace();
            _log.error("Statement close error:	" + sql.getMessage());
        }
    }
    
    private void close_database() {
        try {
            this._conn.close();
        } catch (SQLException sql) {
            sql.printStackTrace();
            _log.error("DB Close error:	" + sql.getMessage());
        }
    }
    /*
     * 
     */
    private int generate_seq_alea(int deb, int fin) {
        if (!this._new_order) {
            for (int i = deb; i <= fin; i++) {
                this._seqIdCustomer[i] = i + 1;
            }
            this._new_order = true;
        }
        int rand = 0;        
        int alea = 0;
        do {
            rand = (int) jTPCCTools.nonUniformRandom(getC_ID(), jTPCCTools.A_C_ID, deb, fin);
            alea = this._seqIdCustomer[rand];
        } while (alea == jTPCCTools.NULL_NUMBER);
        _seqIdCustomer[rand] = jTPCCTools.NULL_NUMBER;
        return alea;
    }


    public String c_last() {
        String c_last = ""; 
        long number = jTPCCTools.nonUniformRandom(getC_LAST(), jTPCCTools.A_C_LAST, jTPCCTools.MIN_C_LAST, jTPCCTools.MAX_C_LAST);
        String alea = String.valueOf(number);
        while (alea.length() < 3) {
        	alea = "0"+alea;
        }
    	for (int i=0; i<3; i++) {
    		c_last += jTPCCTools.C_LAST[Integer.parseInt(alea.substring(i, i+1))];    		
    	}
        return c_last;
    }

    public long getC_LAST() {
        if (POP_C_LAST == jTPCCTools.NULL_NUMBER) {
            POP_C_LAST = jTPCCTools.randomNumber(jTPCCTools.MIN_C_LAST, jTPCCTools.A_C_LAST);
        }
        return POP_C_LAST;
    }    
    
    public long getC_ID() {
        if (POP_C_ID == jTPCCTools.NULL_NUMBER) {
            POP_C_ID = jTPCCTools.randomNumber(0, jTPCCTools.A_C_ID);
        }
        return POP_C_ID;
    }    
    
    public long getOL_I_ID() {
        if (POP_OL_I_ID == jTPCCTools.NULL_NUMBER) {
            POP_OL_I_ID = jTPCCTools.randomNumber(0, jTPCCTools.A_OL_I_ID);
        }
        return POP_OL_I_ID;
    }


    public static void main(String[] args) {
    	// the warehouse number is set to 1 by default
    	int nb_warehouse = 1;
    	if (args[0] != null && !"".equals(args[0])) {
    		nb_warehouse = Integer.parseInt(args[0]);
    	}
        jTPCCPopulation pop = new jTPCCPopulation(nb_warehouse);
        
        pop.close_database();
    }
    
}

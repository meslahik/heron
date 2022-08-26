/*
 * jTPCCTools - Tpc-c population tools
 *
 * Copyright (C) 2005, Mohamed Tlemsani <mtlemsani@users.sourceforge.net>
 *
 * jTPCCTools is free software; you can redistribute it and/or
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

import java.io.UnsupportedEncodingException;
import java.util.Random;

/**
 * @author <a href="mailto:mtlemsani@users.sourceforge.net">Mohamed Tlemsani</a>
 * @version $Id: jTPCCTools.java,v 1.3 2005/07/12 17:01:41 mtl Exp $
 *
 * Utility allowing to provide simple mechanisms of random generation 
 * of character strings or numbers.
 */

public class jTPCCTools {

    public static final double WAREHOUSE_YTD = 300000.00;
    public static final int NB_MAX_DISTRICT = 10;
    public static final long NB_MAX_ITEM = 100000;
    public static final int NB_MAX_CUSTOMER = 3000;
    public static final int NB_MAX_ORDER = 3000;
    public static final String CHAINE_5_1 = "11111";
    public final static int MIN_C_LAST = 0;
    public final static int MAX_C_LAST = 999;
    public final static String[] C_LAST = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
    public final static int LIMIT_ORDER = 2101;    
    public static final int NULL_NUMBER = -1;
    public final static int S_DATA_MINN = 26;
    public final static int A_C_LAST = 255;
    public final static int A_OL_I_ID = 8191;
    public final static int A_C_ID = 1023;
    private final static int DEFAULT_RADIX = 10;
    private final static int DEFAULT_MINL = 65;
    private final static int DEFAULT_MAXL = 90;
    private final static int DEFAULT_MINN = 48;
    private final static int DEFAULT_MAXN = 57;
    private final static int S_DATA_MAXN = 50;
    public final static String ORIGINAL = "ORIGINAL";
    private final static int unicode[][] = { {65, 126}, {192, 259}};
    private static Random _rand = new Random(System.currentTimeMillis());
    
    private static String alea_chaine(int deb, int fin, int min, int max, int radix) {
        if (deb > fin) return null;
        String chaine = new String();        
        int lch = fin;
        
        if (deb != fin) lch = alea_number(deb, fin);
        
        for (int i=0; i<lch; i++) {
            int random = _rand.nextInt(max-min+1)+min;	        
	        char c = (char) (((byte) random) & 0xff);	        
	        chaine += c;
        }   
        return chaine;        
    } 
    
    
    public static String alea_chainel(int deb, int fin, int radix) {
        return alea_chaine(deb, fin, DEFAULT_MINL, DEFAULT_MAXL, radix);        
    }

    public static String alea_chainel(int deb, int fin) {
        return alea_chainel(deb, fin, DEFAULT_RADIX);
    }
    
    
    public static String alea_chainec(int deb, int fin, int radix) {
        if (deb > fin) return null;
        String chaine = "";
		String str = null;
        
        int lch = fin;
        if (deb != fin) lch = alea_number(deb, fin);
        
        for (int i=0; i<lch; i++) {
            int ref = _rand.nextInt(2);
            int min = unicode[ref][0];
            int max = unicode[ref][1];
            int random = _rand.nextInt(max-min+1)+min;
	        
	        char c = (char) (((byte) random));
	        chaine += c;
        }        
        try {
        	str = new String(chaine.toString().getBytes(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
        	System.out.println("----------- Error "+e.getMessage());
        }
        return str;
    }

    public static String alea_chainec(int deb, int fin) {
        return alea_chainec(deb, fin, DEFAULT_RADIX);        
    }

    public static String s_data() {    	
        String alea = alea_chainec(S_DATA_MINN, S_DATA_MAXN);
        if (alea_number(1, 10) == 1) {
        	long number = randomNumber(0, alea.length()-8);
        	alea = alea.substring(0, (int)number)+ORIGINAL+alea.substring((int)number+8, alea.length());
        }
        return alea;
    }
    
    
    public static String alea_chainen(int deb, int fin, int radix) {
        return alea_chaine(deb, fin, DEFAULT_MINN, DEFAULT_MAXN, radix);
    }

    public static String alea_chainen(int deb, int fin) {
        return alea_chainen(deb, fin, DEFAULT_RADIX);
    }
    

    public static int alea_number(int deb, int fin) {
        return _rand.nextInt(fin-deb+1)+deb;
    }
    

    public static long alea_number(long deb, long fin) {
        long random = _rand.nextLong()%(fin+1);
        while (random < deb) random += fin-deb;
        return random;        
    }

    public static float alea_float(float deb, float fin, int virg) {
        if(deb > fin || virg < 1) return 0;
        long pow = (long)Math.pow(10, virg);
        long amin =  (long) (deb * pow);
        long amax =  (long) (fin * pow);
        long random = (long) (_rand.nextDouble() * (amax-amin) + amin);
        return (float) random/pow;
    }

    public static double alea_double(double deb, double fin, int virg) {
        if(deb >= fin || virg < 1) return 0.;
        long pow = (long)Math.pow(10, virg);
        long amin =  (long) (deb * pow);
        long amax =  (long) (fin * pow);
        long random = (long) (_rand.nextDouble() * (amax-amin) + amin);
        return (double) random/pow;        
    }

    public static long randomNumber(long min, long max)
    {
        return (long)(_rand.nextDouble() * (max-min+1) + min); 
    }
    
    public static long nonUniformRandom(long type, long x, long min, long max)
    {
        return (((randomNumber(0, x) | randomNumber(min, max)) + type) % (max-min+1)) + min;
    }
    
}

/*
 *********************************************************************************************************************
 *    FECHA         ID.REQUERIMIENTO            DESCRIPCIÓN                            MODIFICÓ.          
 * 09/02/2015             3226            Creación del documento con            José Manuel Zúñiga Flores 
 *                                        especificaciones de la Carga de       Fernando Vázquez Galán    
 *                                        Archivo para Datos Faltantes de                                 
 *                                        Calificación de Cartera                                         
 *                                                                                                        
 *********************************************************************************************************************
 * 24/03/2015           3329            Carga Masiva Datos Faltantes Línea      José Manuel Zuñiga Flores 
 *                                      Crédito y Código CNBV                                             
 ********************************************************************************************************************* 
 *20/08/2015	4026	Cambio hecho por bajaware, modificacion a la funcionalidad de		José Manuel
 *			Sicc Persona (manual y masiva) para el dato Es Fondo ya que ahora 
 *			es un catálogo.
 *			Modificacion a la funcionalidad de garantías, para que en el combo 
 *			de localidad se despliegue el catalogo utilizado por bajaware.
 *********************************************************************************************************************
 */
package OrionSFI.core.siccCargaMasivaDatosFaltantes;

import OrionSFI.core.commons.InspectorDatos;
import OrionSFI.core.commons.JDBCConnectionPool;
import OrionSFI.core.commons.MensajesSistema;
import OrionSFI.core.commons.SQLProperties;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.spi.DirStateFactory.Result;

public class ValidaCampos {

    private short noInstitucion = 0;
    private String idUsuario = null;
    private String macAddress = null;
    private String folioLote = null;
    private ArchivoError archivoError = null;

    public ValidaCampos() {
    }

    public ValidaCampos(short _noInstitucion, String _idUsuario, String _macAddress, String _folioLote) throws Exception {
        this.noInstitucion = _noInstitucion;
        this.idUsuario = _idUsuario;
        this.macAddress = _macAddress;
        this.folioLote = _folioLote;
        this.archivoError = new ArchivoError(noInstitucion, idUsuario, macAddress, folioLote);
    }

    public void validaCampoRequerido(String nombreCampo, int codigoError, int noLayout, int consecutivo) throws Exception {
        archivoError.insertaError(nombreCampo, codigoError, noLayout, consecutivo);
    }

    public String validaCodigoCNBV(String dato, String nombreCampo, int longitud, int noLayout, int consecutivo, byte indRequerido) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        int codigo;
        if (iData.isStringNULLExt(dato)) {
            if (indRequerido == 1) {
                archivoError.insertaError(nombreCampo, 616, noLayout, consecutivo);
                dato = "-1000000000000000000000000";
            }
        } else {
            codigo = validaAlfanumericos(dato, longitud);
            if (codigo != 0) {
                archivoError.insertaError(nombreCampo, codigo, noLayout, consecutivo);
                dato = "-1000000000000000000000000";
            } else if (iData.verificaLongitud(dato, longitud)) {
                archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                dato = dato.substring(0, longitud);
            } else if (dato.length() < 25) {
                archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                for (int i = dato.length(); i <= 25; i++) {
                    dato += "0";
                }
            }
        }
        return dato;
    }

    public String validaStringNull(String dato, byte isString) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        String cadReturn;

        if (iData.isStringNULLExt(dato)) {
            cadReturn = "NULL";
        } else {
            if (isString == 1) {
                cadReturn = "'" + dato + "'";
            } else {
                cadReturn = dato;
            }
        }
        return cadReturn;
    }
    
    public String convierteNullCero(String dato, byte isString) {
        InspectorDatos iData = new InspectorDatos();
        
        if (iData.isStringNULLExt(dato)) {
            if(isString == 1)
                return "'0.0'";
            else
                return "0.0";
        } else {
            if (isString == 1) {
                return "'" + dato + "'";
            } else {
                return dato;
            }
        }
    }

    public String getCampoBit(String dato, String nombreCampo, int longitud, int noLayout, int consecutivo, String valDefalut, byte indRequerido) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        int codigo;

        if (iData.isStringNULLExt(dato)) {
            if(indRequerido == 1) {
                dato = valDefalut;
            }
        } else {
            codigo = validaBits(dato, longitud);
            if (codigo != 0) {
                archivoError.insertaError(nombreCampo, codigo, noLayout, consecutivo);
                dato = "0";
            } else if (iData.verificaLongitud(dato, longitud)) {
                archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                dato = "0";
            }
        }
        return dato;
    }

    public String getCampoNumerico(String dato, String nombreCampo, int longitud, int noLayout, int consecutivo, byte indRequerido) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        int codigo;
        if (iData.isStringNULLExt(dato)) {
            if (indRequerido == 1) {
                archivoError.insertaError(nombreCampo, 987, noLayout, consecutivo);
                dato = "0";
            }
        } else {
            codigo = validaNumeros(dato, longitud);
            if (codigo != 0) {
                archivoError.insertaError(nombreCampo, codigo, noLayout, consecutivo);
                dato = "0";
            } else if (iData.verificaLongitud(dato, longitud)) {
                archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                dato = dato.substring(0, longitud);
            }
        }
        return dato;
    }
    
    public String getCampoNumericoConsecutivo(String dato, String nombreCampo, int longitud, int noLayout, int consecutivo, byte indRequerido) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        int codigo;
        if (iData.isStringNULLExt(dato)) {
            if (indRequerido == 1) {
                archivoError.insertaError(nombreCampo, 987, noLayout, consecutivo);
                dato = "0";
            }
        } else {
            codigo = validaNumerosConsecutivos(dato, longitud);
            if (codigo != 0) {
                archivoError.insertaError(nombreCampo, codigo, noLayout, consecutivo);
                dato = "0";
            } else if (iData.verificaLongitud(dato, longitud)) {
                archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                dato = dato.substring(0, longitud);
            }
        }
  
        return dato;
    }
    
    public String getCampoNumerico(String dato, String nombreCampo, int longitud, int noLayout, int consecutivo, byte indRequerido,byte indValidaCat) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        int codigo;

        if (iData.isStringNULLExt(dato)) {
            if (indRequerido == 1 && indValidaCat == 1) {
                archivoError.insertaError(nombreCampo, 987, noLayout, consecutivo);
                dato = "-1";
            }
            else if(indRequerido == 1 && indValidaCat == 0){
            	 archivoError.insertaError(nombreCampo, 987, noLayout, consecutivo);
                 dato = "0";
            }
        } else {
            codigo = validaNumeros(dato, longitud);
            if (codigo != 0) {
                archivoError.insertaError(nombreCampo, codigo, noLayout, consecutivo);
                dato = "0";
            } else if (iData.verificaLongitud(dato, longitud)) {
                archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                dato = dato.substring(0, longitud);
            }
        }
        return dato;
    }
    
    public String getCampoNumericoDecimales(String dato, String nombreCampo, int longitud, int noLayout, int consecutivo, int conDecimal, byte indRequerido) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        String complemento, entero;
        if (iData.isStringNULLExt(dato)) {
            if (indRequerido == 1) {
                archivoError.insertaError(nombreCampo, 987, noLayout, consecutivo);
                dato = "0.0";
            }
        } else {
            if (validaNumerosConDecimales(dato) > 0) {
                archivoError.insertaError(nombreCampo, 936, noLayout, consecutivo);
                dato = "0.0";
            } else {
                if (dato.indexOf(".") > 0) {
                    if (iData.verificaLongitudDecimales(dato, longitud - conDecimal, conDecimal)) {
                        archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                        complemento = dato.substring(dato.indexOf(".") + 1).length() > conDecimal ? dato.substring(dato.indexOf("."), dato.indexOf(".") + conDecimal + 1) : dato.substring(dato.indexOf("."));
                        entero = dato.substring(0, dato.indexOf(".")).length() > longitud - conDecimal ? dato.substring(0, longitud - conDecimal) : dato.substring(0, dato.indexOf("."));
                        dato = entero + complemento;
                    }
                } else {
                    if (iData.verificaLongitud(dato, longitud - conDecimal)) {
                        archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                        dato = dato.substring(0, longitud - conDecimal);
                    }
                }
            }
        }
        return dato;
    }
    

    public String getCampoNumericoDecimalesNegativo(String dato, String nombreCampo, int longitud, int noLayout, int consecutivo, int conDecimal, byte indRequerido) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        String complemento, entero, signo = "";
        if (iData.isStringNULLExt(dato)) {
            if (indRequerido == 1) {
                archivoError.insertaError(nombreCampo, 987, noLayout, consecutivo);
                dato = "0.0";
            }
        } else {
            if(dato.startsWith("-")) {
                signo = "-";
                dato = dato.substring(1, dato.length());
            }
            if (validaNumerosConDecimales(dato) > 0) {
                archivoError.insertaError(nombreCampo, 936, noLayout, consecutivo);
                dato = "0.0";
            } else {
                if (dato.indexOf(".") > 0) {
                    if (iData.verificaLongitudDecimales(dato, longitud - conDecimal, conDecimal)) {
                        archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                        complemento = dato.substring(dato.indexOf(".") + 1).length() > conDecimal ? dato.substring(dato.indexOf("."), dato.indexOf(".") + conDecimal + 1) : dato.substring(dato.indexOf("."));
                        entero = dato.substring(0, dato.indexOf(".")).length() > longitud - conDecimal ? dato.substring(0, longitud - conDecimal) : dato.substring(0, dato.indexOf("."));
                        dato = entero + complemento;
                    }
                } else {
                    if (iData.verificaLongitud(dato, longitud - conDecimal)) {
                        archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                        dato = dato.substring(0, longitud - conDecimal);
                    }
                }
            }
        }
        return signo + dato;
    }
    
    public String getCampoNumericoSP(String dato, String nombreCampo, int noLayout, int consecutivo, byte indRequerido) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        
        if (iData.isStringNULLExt(dato)) {
            if (indRequerido == 1) {
                archivoError.insertaError(nombreCampo, 987, noLayout, consecutivo);
                dato = "0.0";
            }
        } else { 
            if(validaNumeroSP(dato) > 0) {
                archivoError.insertaError(nombreCampo, 659, noLayout, consecutivo);
                dato = "0.0";
            }
        }
        return dato;
    }

    public String getCampoAlfanumerico(String dato, String nombreCampo, int longitud, int noLayout, int consecutivo, byte indRequerido) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        int codigo;

        if (iData.isStringNULLExt(dato)) {
            if (indRequerido == 1) {
                archivoError.insertaError(nombreCampo, 987, noLayout, consecutivo);
                dato = "0";
            }
            else {
                dato = " ";
            }
        } else {
            codigo = validaAlfanumericos(dato, longitud);
            if (codigo != 0) {
                archivoError.insertaError(nombreCampo, codigo, noLayout, consecutivo);
                if (indRequerido == 1) {
                    dato = "0";
                }
                else {
                    dato = " ";
                }
            } else if (iData.verificaLongitud(dato, longitud)) {
                archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                dato = dato.substring(0, longitud);
            }
        }
        return dato;
    }
    
    public String getCampoCalificacion(String dato, String nombreCampo, int longitud, int noLayout, int consecutivo, byte indRequerido) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        int codigo;

        if (iData.isStringNULLExt(dato)) {
            if (indRequerido == 1) {
                archivoError.insertaError(nombreCampo, 987, noLayout, consecutivo);
                dato = "0";
            }
            else {
                dato = " ";
            }
        } else {
            codigo = validaCalificacion(dato, longitud);
            if (codigo != 0) {
                archivoError.insertaError(nombreCampo, codigo, noLayout, consecutivo);
                dato = " ";
            } else if (iData.verificaLongitud(dato, longitud)) {
                archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                dato = dato.substring(0, longitud);
            }
        }
        return dato;
    }
    public String getCampoFecha2(String campo, String nombreCampo, int longitud, int noLayout, int consecutivo, byte indRequerido) throws Exception {      
        boolean isFecha = true;
        InspectorDatos iData = new InspectorDatos();
        SQLProperties sqlProperties = new SQLProperties();
    	if (!iData.isStringNULL(campo) && !isFechaValida2(campo)) {
    		archivoError.insertaError(nombreCampo, 987, noLayout, consecutivo);
            isFecha = false;
        }
        if (indRequerido == 1 && !iData.isStringNULL(campo) && isFecha) {
            if (iData.verificaLongitud(campo, longitud)) {
            	archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                campo = campo.substring(0, longitud - 1);
            }
            campo = sqlProperties.convierteFechaFormato(campo, "yyyy/MM/dd", sqlProperties.getFormatoSoloFechaOrion());
        } else {
            if (indRequerido == 1) {
                if (isFecha) {
                	archivoError.insertaError(nombreCampo, 901, noLayout, consecutivo);
                }
                campo = " ";
            } else {
                if (isFecha == false) {
                    campo = "NULL";
                } else {
                    campo = iData.isStringNULL(campo) ? "NULL" : campo;
                }
            }
        }      
        return campo;
    }
    public boolean isFechaValida2(String fecha) {
        try {
            SimpleDateFormat formato = new SimpleDateFormat("yyyy/MM/dd", new Locale("en", "US"));
            formato.setLenient(false);
            formato.parse(fecha);
            return true;
        } catch (Exception e) {
        }
        return false;
    }
    public boolean isFechaValida(String fecha) {
        try {
            // Utiliza el formato dd/MM/yyyy para reflejar el formato esperado en español
        	SimpleDateFormat formato = new SimpleDateFormat("dd/MM/yyyy", new Locale("es", "ES"));
            formato.setLenient(false);
            formato.parse(fecha);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    public String getCampoFecha(String dato, String nombreCampo, int longitud, int noLayout, int consecutivo, byte indRequerido) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        dato = dato.replace("-", "/");

        if (iData.isStringNULLExt(dato)) {
            if (indRequerido == 1) {
                archivoError.insertaError(nombreCampo, 987, noLayout, consecutivo);
                dato = "31/12/1999";
            }
            
            
        } else {
            if (!isFechaValida(dato)) {
                archivoError.insertaError(nombreCampo, 904, noLayout, consecutivo);
                dato = "31/12/1999";
            } else if (iData.verificaLongitud(dato, longitud)) {
                archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                dato = "31/12/1999";
            }
        }
        return dato;
    }
    
    public String getCampoFecha(String dato, String nombreCampo, int longitud, int noLayout, int consecutivo, byte fDefault, byte indRequerido) throws Exception {
        InspectorDatos iData = new InspectorDatos();
        dato = dato.replace("-", "/");

        if (iData.isStringNULLExt(dato)) {
            if (indRequerido == 1) {
                archivoError.insertaError(nombreCampo, 987, noLayout, consecutivo);
                dato = "31/12/1999";
            }else if (fDefault == 1) {
                dato = "31/12/1999";
            }
            
            
        } else {
            if (!validaFecha(dato)) {
                archivoError.insertaError(nombreCampo, 904, noLayout, consecutivo);
                dato = "31/12/1999";
            } else if (iData.verificaLongitud(dato, longitud)) {
                archivoError.insertaError(nombreCampo, 903, noLayout, consecutivo);
                dato = "31/12/1999";
            }
        }
        return dato;
    }

    public boolean validaFecha(String dato) {
        if (dato.trim().matches("[0-9]{2}+(/)+[0-9]{2}+(/)+[0-9]{4}")) {
			String[] arrayFecha = dato.split("/");
			if ((Integer.parseInt(arrayFecha[0]) <= 31) && (arrayFecha[0].length() == 2) && (Integer.parseInt(arrayFecha[0]) != 0)
					&& (Integer.parseInt(arrayFecha[1]) <= 12) && (arrayFecha[1].length() == 2) && (Integer.parseInt(arrayFecha[1]) != 0)
					&& (Integer.parseInt(arrayFecha[2]) > 1900) && (Integer.parseInt(arrayFecha[2]) < 3000) && (arrayFecha[2].length() == 4)) {
				if ((Integer.parseInt(arrayFecha[1]) == 1 || Integer.parseInt(arrayFecha[1]) == 3 || Integer.parseInt(arrayFecha[1]) == 5 || Integer.parseInt(arrayFecha[1]) == 1
						|| Integer.parseInt(arrayFecha[1]) == 7 || Integer.parseInt(arrayFecha[1]) == 8 || Integer.parseInt(arrayFecha[1]) == 10 || Integer.parseInt(arrayFecha[1]) == 12)
						
						|| (Integer.parseInt(arrayFecha[1]) != 2 && Integer.parseInt(arrayFecha[0]) <= 30)
						|| (Integer.parseInt(arrayFecha[1]) == 2 && ((Integer.parseInt(arrayFecha[0]) <= 28 && !(Integer.parseInt(arrayFecha[2]) % 4 == 0 && (Integer
								.parseInt(arrayFecha[2]) % 100 != 0 || Integer.parseInt(arrayFecha[2]) % 400 == 0))) || (Integer.parseInt(arrayFecha[0]) <= 29
								&& Integer.parseInt(arrayFecha[2]) % 4 == 0 && (Integer.parseInt(arrayFecha[2]) % 100 != 0 || Integer.parseInt(arrayFecha[2]) % 400 == 0))))) {
					return true;
				}
			}
		}
		return false;
    }

    public int validaBits(String cadena, int longitud) throws Exception {
        int rsCodigo;
        Pattern p = Pattern.compile("(0|1){0," + longitud + "}");
        Matcher m = p.matcher(cadena);
        boolean resultado = m.matches();

        if (resultado == true) {
            rsCodigo = 0;
        } else {
            rsCodigo = 936;
        }
        return rsCodigo;
    }

    public int validaNumeros(String cadena, int longitud) throws Exception {
        int rsCodigo;
        Pattern p = Pattern.compile("[0-9]{0,300}");
        Matcher m = p.matcher(cadena);
        boolean resultado = m.matches();

        if (resultado == true) {
            rsCodigo = 0;
        } else {
            rsCodigo = 936;
        }
        return rsCodigo;
    }

    public int validaNumerosConDecimales(String cadena) {
        int retorna;
        if (cadena.indexOf(".") > 0) {
            if (cadena.trim().matches("-?[0-9]{0,50}+(\\.[0-9]{0,50})")) {
                retorna = 0;
            } else {
                retorna = 936;
            }
        } else {
            if (cadena.trim().matches("^-?\\d{1,150}+")) {
                retorna = 0;
            } else {
                retorna = 936;
            }
        }
        return retorna;
    }

    public int validaAlfanumericos(String cadena, int longitud) throws Exception {
        int rsCodigo;
        Pattern p = Pattern.compile("[a-zA-Z0-9-_ ñáéíóúÑÁÉÍÓÚ\\.nÑ ]{0,2000}");
        Matcher m = p.matcher(cadena.trim());
        boolean resultado = m.matches();

        if (resultado == true) {
            rsCodigo = 0;
        } else {
            rsCodigo = 936;
        }
        return rsCodigo;
    }
    
    public int validaCalificacion(String cadena, int longitud) throws Exception {
        int rsCodigo;
        Pattern p = Pattern.compile("[a-zA-Z0-9-_ +]{0,250}");
        Matcher m = p.matcher(cadena.trim());
        boolean resultado = m.matches();

        if (resultado == true) {
            rsCodigo = 0;
        } else {
            rsCodigo = 936;
        }
        return rsCodigo;
    }
    
    public int validaNumeroSP(String cadena) {
        Pattern p = Pattern.compile("(\\d{0,4}|\\d{0,4}+\\.+\\d{0,6})|([S|s]{1}[P|p]{1})");
        Matcher m = p.matcher(cadena.trim());
        boolean resultado = m.matches();
        
        if(resultado)
            return 0;
        else
            return 659;
    }
    
    public int validaNumerosConsecutivos(String cadena, int longitud) throws Exception {
        int rsCodigo;
        Pattern p = Pattern.compile("(123456789)?");
        Matcher m = p.matcher(cadena);
        Pattern pa = Pattern.compile("(.)\\1{7}|(.)\\2{7}|(.)\\3{7}|(.)\\4{7}|(.)\\5{7}|(.)\\6{7}|(.)\\7{7}|(.)\\8{7}|(.)\\9{7}");
        Matcher ma = p.matcher(cadena);
        boolean resultado = m.matches();
        boolean resultado2 = ma.matches();

        if (resultado == true || resultado2 == true) {
            rsCodigo = 0;
        } else {
            rsCodigo = 936;
        }
        return rsCodigo;
    }
    
    public byte getRequeridoCampo(short numeroInstitucion, String numeroLayout, String numeroCampo) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        SQLProperties sqlProperties = new SQLProperties();
        byte requerido = 0;
        JDBCConnectionPool bd = new JDBCConnectionPool();
        try {
            StringBuffer query = new StringBuffer("");
            query.append("SELECT IND_REQUERIDO ");
            query.append("FROM SICC_CAMPOS_LAYOUT ");
            query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion).append("");
            query.append(" AND NUMERO_LAYOUT = '").append(numeroLayout).append("'");
            query.append(" AND NUMERO_CAMPO = '").append(numeroCampo).append("'");
            System.out.println(query);
            ResultSet resultado = bd.executeQuery(query.toString());
            if(resultado.next())
            {
            	requerido = resultado.getByte("IND_REQUERIDO");
            }
            
            bd.close();
        } catch (SQLException se) {
            bd.close();
            se.printStackTrace();
            throw new SQLException(msjSistema.getMensaje(131));
        }
        return requerido;
    }
    
    /*public static void main(String arg[]) throws Exception {
    	
//    	numero1%numero2 == 0
    			System.out.println("Resultado" + (4%1));
    			System.out.println("Resultado" + (2018%4));
    			System.out.println("Resultado" + (2016%4));
    	
//        ValidaCampos obj = new ValidaCampos();
//        System.out.println(obj.getCampoNumericoDecimales("12345.12", "PRUEBA", 7, 2, 1, 2, (byte)1));
    }*/
}

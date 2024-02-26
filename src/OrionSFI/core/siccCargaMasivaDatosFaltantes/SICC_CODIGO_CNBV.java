package OrionSFI.core.siccCargaMasivaDatosFaltantes;

import OrionSFI.core.commons.InspectorDatos;
import OrionSFI.core.commons.JDBCConnectionPool;
import OrionSFI.core.commons.MensajesSistema;
import OrionSFI.core.commons.SQLProperties;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

/**********************************************************************************************************
 *    FECHA         ID.REQUERIMIENTO            DESCRIPCIÓN                            MODIFICÓ.          *
 * 13/03/2015             3237            Creación del documento con            José Manuel Zúñiga Flores *
 *                                        especificaciones de la Carga de       Fernando Vázquez Galán    *
 *                                        Archivo para Datos Faltantes de                                 *
 *                                        Calificación de Cartera (Linea                                  *
 *                                        Crédito y Código CNBV)                                          *
 **********************************************************************************************************
 * 24/03/2015           3329            Carga Masiva Datos Faltantes Línea      José Manuel Zuñiga Flores *
 *                                      Crédito y Código CNBV                                             *
 **********************************************************************************************************/
public class SICC_CODIGO_CNBV {
    
    private JDBCConnectionPool bd = null;
    private StringBuffer query = null;
    private ValidaCampos validaCampos = null;
    
    /**
     * Valida cada registro.
     * @param registro
     * @return
     * @throws Exception 
     */
    public boolean validaRegistro(String registro) throws Exception {
        boolean valido = false;
        
        StringBuffer expRegular = new StringBuffer();
        expRegular.append("\\d{1,15}+\\|");                                     //NUMERO_LINEA
	expRegular.append("[a-zA-Z0-9-_ ]{25,35}+\\|");                         //CODIGO_CNBV
        expRegular.append("(0?[1-9]|[12][0-9]|3[01])?(/|-)?(0?[1-9]|1[012])?(/|-)?((19|20)\\d\\d)?");//FECHA_CARGA
	
        
        if (registro.toString().matches(expRegular.toString())) {
            valido = true;
        }
        
        return valido;
    }
    
    public String separaRegistros(String registro, int consecutivo, int noLayout, String folioLote, short numeroInstitucion, String idUsuario, String macAddress) throws Exception{
        validaCampos = new ValidaCampos(numeroInstitucion, idUsuario, macAddress, folioLote);
        InspectorDatos iData = new InspectorDatos();
        StringBuffer errRegistro = new StringBuffer();
        StringTokenizer st = new StringTokenizer(registro, "|");
        char pipe = '|';
        String []cad = registro.split("\\|");   
        
        errRegistro.setLength(0);
        errRegistro.append(consecutivo).append(pipe);
        
        if(iData.isStringNULLExt(cad[0]))
        	if(cad[0].equalsIgnoreCase("NULL"))
        		errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_LINEA", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
        	else
        		errRegistro.append(validaCampos.getCampoNumerico(cad[0], "NUMERO_LINEA", 15, noLayout, consecutivo, (byte)1)).append(pipe);
        else errRegistro.append(validaCampos.getCampoNumerico(st.nextToken(), "NUMERO_LINEA", 15, noLayout, consecutivo, (byte) 1)).append(pipe);
        
        errRegistro.append(validaCampos.validaCodigoCNBV(st.nextToken(), "CODIGO_CNBV", 35, noLayout, consecutivo, (byte) 1)).append(pipe);
        errRegistro.append(validaCampos.getCampoFecha(st.nextToken(), "FECHA_CARGA", 10, noLayout, consecutivo, (byte) 1)).append(pipe);
        
        return errRegistro.toString();
    }
    
    public void insertaCarga(String registro, short numeroInstitucion, String nombreArchivo, String folioLote, int noLayout, String idUsuario, String macAddress) throws Exception{
        MensajesSistema msjSistema = new MensajesSistema();
        StringTokenizer st = new StringTokenizer(registro, "|");
        
        SQLProperties sqlProperties = new SQLProperties();
        SimpleDateFormat sdf = new SimpleDateFormat(sqlProperties.getFormatoSoloFechaOrion());
        String fechaAlta = sqlProperties.getFechaInsercionFormateada(sdf.format(new Date()));
        
        try{   
            bd = new JDBCConnectionPool();
            bd.setAutoCommit(false);
            
            query = new StringBuffer();
            query.append("INSERT INTO dbo.SICC_CODIGO_CNBV_LO(");
            query.append("  NUMERO_INSTITUCION");
            query.append(", NOMBRE_ARCHIVO");
            query.append(", FOLIO_LOTE");
            query.append(", NUMERO_LAYOUT");
            query.append(", NUMERO_CONSECUTIVO");
            query.append(", NUMERO_LINEA");
            query.append(", CODIGO_CNBV");
            query.append(", FECHA_CARGA");
            query.append(", STATUS_CARGA");
            query.append(", STATUS_ALTA");
            query.append(", STATUS");
            query.append(", FECHA_ALTA");
            query.append(", ID_USUARIO_ALTA");
            query.append(", MAC_ADDRESS_ALTA");
            query.append(", FECHA_MODIFICACION");
            query.append(", ID_USUARIO_MODIFICACION");
            query.append(", MAC_ADDRESS_MODIFICACION) ");
            query.append("VALUES(");
            query.append("  ").append(numeroInstitucion);
            query.append(", '").append(nombreArchivo).append("'");
            query.append(", '").append(folioLote).append("'");
            query.append(", ").append(noLayout);
            query.append(", ").append(st.nextToken());                          //NUMERO_CONSECUTIVO
            query.append(", ").append(st.nextToken());                          //NUMERO_LINEA
            query.append(", '").append(st.nextToken()).append("'");             //CODIGO_CNBV
            query.append(", '").append(st.nextToken()).append("'");             //FECHA_CARGA
            query.append(", 'NP'");
            query.append(", 'NP'");
            query.append(", 1");
            query.append(", '").append(fechaAlta).append("'");
            query.append(", '").append(idUsuario).append("'");
            query.append(", '").append(macAddress).append("'");
            query.append(", NULL");
            query.append(", NULL");
            query.append(", NULL)");
            System.out.println(query);
            bd.executeStatement(query.toString());
            
            bd.commit();
            bd.close();
        }catch(SQLException e){
            bd.rollback();
            bd.close();
            e.printStackTrace();
            throw new Exception(msjSistema.getMensaje(131));
        }
    }
    
    public void procesoAltaModificacion(short numeroInstitucion, int noLayout, String fechaProceso, String folioLote, String idUsuario, String macAddress)throws Exception{
        MensajesSistema msjSistema = new MensajesSistema();
        SQLProperties sqlProperties = new SQLProperties();
        UpdateCommon updateCommon = new UpdateCommon();
        List<String> listCampos = new ArrayList<String>();
        List<String> listDatos = new ArrayList<String>();
        ResultSet resultadoCampos = null;
        ResultSet resultadoDatos = null;
        int consecutivo = 0;
        
        try{
            bd = new JDBCConnectionPool();
            
            query = new StringBuffer("SELECT ");
            query.append("UPPER(ISC.DATA_TYPE) AS TIPO_DATO, ");
            query.append("ISC.COLUMN_NAME AS CAMPO ");
            query.append("FROM SICC_CAMPOS_LAYOUT AS SCL ");
            query.append("INNER JOIN INFORMATION_SCHEMA.COLUMNS ISC ON ");
            query.append("ISC.COLUMN_NAME=SCL.NOMBRE_CAMPO ");
            query.append("AND ISC.TABLE_NAME = 'SICC_CODIGO_CNBV' ");
            query.append("AND SCL.IND_ACTUALIZA = 1 ");
            query.append("AND SCL.NUMERO_INSTITUCION = ").append(numeroInstitucion);
            query.append(" AND SCL.NUMERO_LAYOUT = ").append(noLayout);
            System.out.println(query);
            resultadoCampos = bd.executeQuery(query.toString());
            listCampos = sqlProperties.getColumnValue(resultadoCampos, listCampos);
            String campos = null;
            int i = 0;
            
            query.setLength(0);
            query.append("SELECT STL.NUMERO_LINEA, ");
            query.append("STL.NUMERO_CONSECUTIVO, ");
            while(i < listCampos.size()-1){ 
                campos = listCampos.get(i).toString();
                campos = campos.substring(campos.indexOf("&#")+2, campos.length());
                query.append("STL.").append(campos).append(", ");
                i++;
            }
            campos = listCampos.get(listCampos.size()-1).toString();
            campos = campos.substring(campos.indexOf("&#")+2, campos.length());
            query.append("STL.").append(campos);
            query.append(" FROM SICC_CODIGO_CNBV_LO AS STL ");
            query.append("WHERE STL.NUMERO_LINEA IS NOT NULL ");
            query.append("AND EXISTS( ");
            query.append("SELECT SFT.NUMERO_LINEA FROM SICC_CODIGO_CNBV AS SFT ");
            query.append("WHERE STL.NUMERO_INSTITUCION = SFT.NUMERO_INSTITUCION ");
            query.append("AND STL.NUMERO_LINEA = SFT.NUMERO_LINEA) ");
            query.append("AND STL.NUMERO_INSTITUCION = ").append(numeroInstitucion);
            query.append(" AND STL.STATUS_CARGA = 'P' ");
            query.append("AND STL.FOLIO_LOTE = '").append(folioLote).append("'");
            System.out.println(query);
            resultadoDatos = bd.executeQuery(query.toString());
            listDatos = sqlProperties.getColumnValue(resultadoDatos, listDatos);
            
              /* SP ALTA_DATOS_FALTANTES_SICC */
            try{
                bd.setAutoCommit(false);
                System.out.println("--------------------------------------------ALTA_DATOS_FALTANTES_SICC--------------------------------------------");
                query.setLength(0);
                query.append("ALTA_DATOS_FALTANTES_SICC(");
                query.append(numeroInstitucion);
                query.append(", ").append(noLayout);
                query.append(", '").append(fechaProceso).append("'");
                query.append(", '").append(folioLote).append("'");
                query.append(", '").append(idUsuario).append("'");
                query.append(", '").append(macAddress).append("')");
                System.out.println(query);
                bd.executeStoreProcedure(query.toString());
                
                bd.commit();
            } catch(SQLException e){
                bd.rollback();
                bd.close();
                e.printStackTrace();
                throw new Exception(msjSistema.getMensaje(131));                
            }
            
            i = 0;
            Date fechaActual = new Date();
            SimpleDateFormat formato = new SimpleDateFormat(sqlProperties.getFormatoFecha());
            String fechaModificacion = formato.format(fechaActual);
            
            while(i<listDatos.size()){
                try{
                    StringTokenizer stD = new StringTokenizer(listDatos.get(i).toString(), "&#");
                    String numeroLinea = stD.nextToken();
                    consecutivo = Integer.parseInt(stD.nextToken());
                    bd.setAutoCommit(false);
                    query.setLength(0);
                    query.append("INSERT INTO LOG_SICC_CODIGO_CNBV ");
                    query.append("SELECT ");
                    query.append("NUMERO_INSTITUCION, ");
                    query.append("NUMERO_LINEA, '");
                    query.append(fechaModificacion);
                    query.append("', CODIGO_CNBV, ");
                    query.append("FECHA_CARGA, ");
                    query.append("STATUS, '");
                    query.append(idUsuario).append("', '");
                    query.append(macAddress);
                    query.append("' FROM SICC_CODIGO_CNBV ");
                    query.append("WHERE NUMERO_INSTITUCION = ");
                    query.append(numeroInstitucion);
                    query.append(" AND NUMERO_LINEA = ");
                    query.append(numeroLinea);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());

                    query.setLength(0);
                    query.append("UPDATE SICC_CODIGO_CNBV SET ");
                    query.append(updateCommon.armaUpdate(listCampos, stD));
                    query.append(", FECHA_MODIFICACION = '").append(fechaModificacion).append("', ");
                    query.append("ID_USUARIO_MODIFICACION = '").append(idUsuario).append("', ");
                    query.append("MAC_ADDRESS_MODIFICACION = '").append(macAddress).append("' ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append(" AND NUMERO_LINEA = ").append(numeroLinea);
                    System.out.println(query);
                    bd.executeUpdate(query.toString());
                    
                    bd.commit();
                }catch(SQLException e){
                    bd.rollback();
                    e.printStackTrace();
                    query.setLength(0);
                    query.append("UPDATE SICC_CODIGO_CNBV_LO ");
                    query.append("SET  STATUS_ALTA = 'PP' ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append(" AND FOLIO_LOTE = '").append(folioLote).append("' ");
                    query.append("AND NUMERO_CONSECUTIVO = ").append(consecutivo);
                    query.append(" AND STATUS_CARGA = 'P'");
                    System.out.println(query);
                    bd.executeUpdate(query.toString());
                    
                    ArchivoError archivoError = new ArchivoError(numeroInstitucion, idUsuario, macAddress, folioLote);
                    archivoError.insertaError(String.valueOf(consecutivo), 989, noLayout, consecutivo);
                }
                i++;
            }     
            bd.close();
        }catch(SQLException e){
            bd.close();
            e.printStackTrace();
            throw new Exception(msjSistema.getMensaje(131));
        }
    }
}

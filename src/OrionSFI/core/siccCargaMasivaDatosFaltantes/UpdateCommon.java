/*
 **********************************************************************************************************
 *    FECHA         ID.REQUERIMIENTO            DESCRIPCIÓN                            MODIFICÓ.          
 * 09/02/2015             3226            Creación del documento con            José Manuel Zúñiga Flores 
 *                                        especificaciones de la Carga de       Fernando Vázquez Galán    
 *                                        Archivo para Datos Faltantes de                                 
 *                                        Calificación de Cartera                                         
 *                                                                                                        
 **********************************************************************************************************
 * 29/04/2016           5620        Adecuaciones Carga Masiva Datos Faltantes   José Manuel
 *                                  (Calificación de Cartera)
 **********************************************************************************************************
 */
package OrionSFI.core.siccCargaMasivaDatosFaltantes;

import OrionSFI.core.commons.InspectorDatos;
import OrionSFI.core.commons.MensajesSistema;
import java.math.BigDecimal;
import java.util.List;
import java.util.StringTokenizer;

public class UpdateCommon {

    private StringBuffer query = null;
    private ValidaCampos validaCampos = new ValidaCampos();
    private InspectorDatos iDat = new InspectorDatos();

    public String armaUpdate(List<String> columnas, StringTokenizer stD) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        query = new StringBuffer();
        int indColumna = 0;
        String tipoDato;
        try {
            while (indColumna < columnas.size()) {
                StringTokenizer stC = new StringTokenizer(columnas.get(indColumna), "&#");
                tipoDato = stC.nextToken();

                if (indColumna == 0) {
                    switch (tipoDato) {
                        case "NUMERIC":
                            query.append(" ").append(stC.nextToken()).append(" = ").append(validaCampos.validaStringNull(stD.nextToken(), (byte) 0));
                            break;
                        case "BIT":
                            query.append(" ").append(stC.nextToken()).append(" = ").append(validaCampos.validaStringNull(stD.nextToken(), (byte) 0));
                            break;
                        case "VARCHAR":
                            query.append(" ").append(stC.nextToken()).append(" = ").append(validaCampos.validaStringNull(stD.nextToken(), (byte) 1));
                            break;
                        case "DATETIME":
                            query.append(" ").append(stC.nextToken()).append(" = ").append(validaCampos.validaStringNull(stD.nextToken(), (byte) 1));
                            break;
                        default: break;
                    }
                } else {
                    switch (tipoDato) {
                        case "NUMERIC":
                            query.append(" ,").append(stC.nextToken()).append(" = ").append(validaCampos.validaStringNull(stD.nextToken(), (byte) 0));
                            break;
                        case "BIT":
                            query.append(" ,").append(stC.nextToken()).append(" = ").append(validaCampos.validaStringNull(stD.nextToken(), (byte) 0));
                            break;
                        case "VARCHAR":
                            query.append(" ,").append(stC.nextToken()).append(" = ").append(validaCampos.validaStringNull(stD.nextToken(), (byte) 1));
                            break;
                        case "DATETIME":
                            query.append(" ,").append(stC.nextToken()).append(" = ").append(validaCampos.validaStringNull(stD.nextToken(), (byte) 1));
                            break;
                        default: break;
                    }
                }
                indColumna++;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(msjSistema.getMensaje(131));
        }
        return query.toString();
    }

    public String armaUpdateCredito(List<String> columnas, StringTokenizer stD, byte indAlta, byte indBaja) throws Exception {
        query = new StringBuffer();
        int indColumna = 0;
        String tipoDato;
        String nombreColumna;
        BigDecimal totalMonto = new BigDecimal("0.0");
        String comisionesCobradas = null;
        String comDispMonto = null;
        String comisionAnualidad = null;
        String sdoQuitas = null;
        String sdoCastigos = null;
        String sdoCondonacion = null;

        String tipoAlta = null;
        String tipoBaja = null;

        try {
            while (indColumna < columnas.size()) {
                StringTokenizer stC = new StringTokenizer(columnas.get(indColumna), "&#");
                tipoDato = stC.nextToken();
                nombreColumna = stC.nextToken();

                if (!nombreColumna.equals("COMISIONES_COBRADAS") && !nombreColumna.equals("COMISION_DISPOSICION_MONTO")
                        && !nombreColumna.equals("COMISION_ANUALIDAD") && !nombreColumna.equals("MONTO_PAGO_EFECTIVO_COMISION")
                        && !nombreColumna.equals("SDO_QUITAS") && !nombreColumna.equals("SDO_CASTIGOS") 
                        && !nombreColumna.equals("SDO_CONDONACION")
                        && !nombreColumna.equals("TIPO_ALTA")
                        && !nombreColumna.equals("TIPO_BAJA")
                		) {
                    
                    if (indColumna == 0) {
                        query.append("  ");
                    } else {
                        query.append("  ,");
                    }
                    
                    switch (tipoDato) {
                        case "NUMERIC":
                            query.append(nombreColumna).append(" = ").append(validaCampos.validaStringNull(stD.nextToken(), (byte) 0));
                            break;
                        case "BIT":
                            query.append(nombreColumna).append(" = ").append(validaCampos.validaStringNull(stD.nextToken(), (byte) 0));
                            break;
                        case "VARCHAR":
                        case "DATETIME":
                            query.append(nombreColumna).append(" = ").append(validaCampos.validaStringNull(stD.nextToken(), (byte) 1));
                            break;
                        default: break;
                    }
                    indColumna++;
                } else {
                    switch (nombreColumna) {
                        case "COMISIONES_COBRADAS":
                            comisionesCobradas = validaCampos.validaStringNull(stD.nextToken(), (byte) 0);
                            break;
                        case "COMISION_DISPOSICION_MONTO":
                            comDispMonto = validaCampos.validaStringNull(stD.nextToken(), (byte) 0);
                            break;
                        case "COMISION_ANUALIDAD":
                            comisionAnualidad = validaCampos.validaStringNull(stD.nextToken(), (byte) 0);
                            break;
                        case "SDO_QUITAS":
                            sdoQuitas = validaCampos.validaStringNull(stD.nextToken(), (byte) 0);
                            break;
                        case "SDO_CASTIGOS":
                            sdoCastigos = validaCampos.validaStringNull(stD.nextToken(), (byte) 0);
                            break;
                        case "SDO_CONDONACION":
                            sdoCondonacion = validaCampos.validaStringNull(stD.nextToken(), (byte) 0);
                            break;   
                        case "TIPO_ALTA":
                        	tipoAlta = validaCampos.validaStringNull(stD.nextToken(), (byte) 0);
                        	break;   
                        case "TIPO_BAJA":
                        	tipoBaja = validaCampos.validaStringNull(stD.nextToken(), (byte) 0);
                        	break;   
                        default: break;
                    }
                    indColumna++;
                }
            }
            
			if (indAlta == 1) {
				query.append("  ,TIPO_ALTA = ").append(tipoAlta);
			}
			if (indBaja == 1) {
				query.append("  ,TIPO_BAJA = ").append(tipoBaja);
			}
            query.append("  ,COMISIONES_COBRADAS = ").append(iDat.isStringNULLExt(comisionesCobradas) ? "NULL" : comisionesCobradas);
            query.append("  ,COMISION_DISPOSICION_MONTO = ").append(iDat.isStringNULLExt(comDispMonto) ? "NULL" : comDispMonto);
            query.append("  ,COMISION_ANUALIDAD = ").append(iDat.isStringNULLExt(comisionAnualidad) ? "NULL" : comisionAnualidad);
            query.append("  ,SDO_QUITAS = ").append(iDat.isStringNULLExt(sdoQuitas) ? "NULL" : sdoQuitas);
            query.append("  ,SDO_CASTIGOS = ").append(iDat.isStringNULLExt(sdoCastigos) ? "NULL" : sdoCastigos);
            query.append("  ,SDO_CONDONACION = ").append(iDat.isStringNULLExt(sdoCondonacion) ? "NULL" : sdoCondonacion);
            
            totalMonto = totalMonto.add(new BigDecimal(iDat.isStringNULLExt(comisionesCobradas) ? "0" : comisionesCobradas)).add(new BigDecimal(iDat.isStringNULLExt(comDispMonto) ? "0" : comDispMonto)).add(new BigDecimal(iDat.isStringNULLExt(comisionAnualidad) ? "0" : comisionAnualidad));
            query.append("  ,MONTO_PAGO_EFECTIVO_COMISION = ").append(totalMonto);
            
            totalMonto = new BigDecimal("0.0");
            totalMonto = totalMonto.add(new BigDecimal(iDat.isStringNULLExt(sdoQuitas) ? "0" : sdoQuitas)).add(new BigDecimal(iDat.isStringNULLExt(sdoCastigos) ? "0" : sdoCastigos)).add(new BigDecimal(iDat.isStringNULLExt(sdoCondonacion) ? "0" : sdoCondonacion));
            query.append("  ,SDO_QUEBRANTO = ").append(totalMonto);
            
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(new MensajesSistema().getMensaje(131));
        }
        return query.toString();
    }
}

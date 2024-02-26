/*
 **********************************************************************************************************
 *    FECHA         ID.REQUERIMIENTO            DESCRIPCIÓN                            MODIFICÓ.          
 * 09/02/2015             3226            Creación del documento con            José Manuel Zúñiga Flores 
 *                                        especificaciones de la Carga de       Fernando Vázquez Galán    
 *                                        Archivo para Datos Faltantes de                                 
 *                                        Calificación de Cartera                                         
 *                                                                                                        
 **********************************************************************************************************
 * 24/03/2015           3329            Carga Masiva Datos Faltantes Línea      José Manuel Zuñiga Flores 
 *                                      Crédito y Código CNBV                                             
 **********************************************************************************************************
 * 29/04/2016           5620        Adecuaciones Carga Masiva Datos Faltantes   José Manuel
 *                                  (Calificación de Cartera)
 **********************************************************************************************************
 */
package OrionSFI.core.siccCargaMasivaDatosFaltantes;

import OrionSFI.core.commons.Cifrador;
import OrionSFI.core.commons.JDBCConnectionPool;
import OrionSFI.core.commons.MensajesSistema;
import OrionSFI.core.commons.SQLProperties;
import OrionSFI.core.institucion.Institucion;
import OrionSFI.core.sistema.Sistema;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.io.FileUtils;

public class SiccCargaMasivaDatosFaltantes {

    private JDBCConnectionPool bd = null;
    private StringBuffer query = null;
    private ResultSet resultado = null;
    private List<String> listData = new ArrayList<>();
    private SQLProperties sqlProperties = new SQLProperties();
    private SICC_PERSONA persona = new SICC_PERSONA();
    private SICC_CREDITO credito = new SICC_CREDITO();
    private SICC_ANEXO19 anexo19 = new SICC_ANEXO19();
    private SICC_ANEXO20 anexo20 = new SICC_ANEXO20();
    private SICC_ANEXO21 anexo21 = new SICC_ANEXO21();
    private SICC_ANEXO22 anexo22 = new SICC_ANEXO22();
    private SICC_AVAL aval = new SICC_AVAL();
    private SICC_LINEA_CREDITO lineaCredito = new SICC_LINEA_CREDITO();
    private SICC_CODIGO_CNBV codigoCNBV = new SICC_CODIGO_CNBV();
    private String fechaProceso = null;
    private SICC_GARANTIAS garantias = new SICC_GARANTIAS();
    private SICC_BIENES_ADJUDICADOS bienes = new SICC_BIENES_ADJUDICADOS();
    private SICC_OPERATIVO_INTERBANCARIO interbancario = new SICC_OPERATIVO_INTERBANCARIO();
    private SICC_OPERATIVO_PRESTAMO prestamo = new SICC_OPERATIVO_PRESTAMO();
    private SICC_CATALOGO_MINIMO catalogoMinimo = new SICC_CATALOGO_MINIMO();
    private SICC_OPERATIVOS_INVERSIONES inversiones = new SICC_OPERATIVOS_INVERSIONES();
    private SICC_CONTABLES_RECLASIFICACIONES reclasificaciones = new SICC_CONTABLES_RECLASIFICACIONES();
    private SICC_CONTABLES_UPA cotablesUPA = new SICC_CONTABLES_UPA();
    private SICC_CONTABLES_CONSOLIDACION consolidacion = new SICC_CONTABLES_CONSOLIDACION();
    private SICC_CONTABLES_ELIMINACIONES eliminaciones = new SICC_CONTABLES_ELIMINACIONES();
    private SICC_ESTADO_VARIACIONES estadoVariaciones = new SICC_ESTADO_VARIACIONES();
    private SICC_ESTADO_FLUJO_EFECTIVO flujoEfectivo = new SICC_ESTADO_FLUJO_EFECTIVO();
    private SICC_BALANCE_GENERAL balanceGeneral = new SICC_BALANCE_GENERAL();
    private SICC_ESTADO_RESULTADOS estadoResultados = new SICC_ESTADO_RESULTADOS();
    private SICC_ESTADO_CUENTA estadoCuenta = new SICC_ESTADO_CUENTA();
    private static String OS = System.getProperty("os.name").toLowerCase();
    
    public SiccCargaMasivaDatosFaltantes() {
    }
    
    public SiccCargaMasivaDatosFaltantes(String fechaProceso) {
        this.fechaProceso = fechaProceso;
    }

    private int countOccurrences(String registro, char caracter, int i) {
        return ((i = registro.indexOf(caracter, i)) == -1) ? 0 : 1 + countOccurrences(registro, caracter, i + 1);
    }

    public List<String> getDatosLayout(short numInstitucion) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        sqlProperties = new SQLProperties();

        try {
            bd = new JDBCConnectionPool();

            query = new StringBuffer();
            query.append("SELECT DISTINCT SICL.NUMERO_LAYOUT ");
            query.append("  ,SCL.NOMBRE_LAYOUT ");
            query.append("  ,SCL.STATUS ");
            query.append("FROM SICC_CATALOGO_LAYOUT AS SCL ");
            query.append("INNER JOIN SICC_CAMPOS_LAYOUT AS SICL ON SCL.NUMERO_INSTITUCION = SICL.NUMERO_INSTITUCION ");
            query.append("  AND SCL.NUMERO_LAYOUT = SICL.NUMERO_LAYOUT ");
            query.append("WHERE SCL.NUMERO_INSTITUCION = ").append(numInstitucion);
            System.out.println(query);
            resultado = bd.executeQuery(query.toString());

            listData = sqlProperties.getColumnName(resultado);
            listData.add("0" + sqlProperties.getSeparador() + sqlProperties.getMsgSeleccioneUno() + sqlProperties.getSeparador() + "1");
            listData = sqlProperties.getColumnValue(resultado, listData);

            bd.close();
        } catch (SQLException e) {
            bd.close();
            e.printStackTrace();
            throw new SQLException(msjSistema.getMensaje(131));
        }
        return listData;
    }

    public List<String> getDatosServidorArchivos(String direccionIP) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        sqlProperties = new SQLProperties();
        String separador = sqlProperties.getSeparador();

        try {
            bd = new JDBCConnectionPool();
            query = new StringBuffer("");
            query.append("SELECT PATH ");
            query.append("  ,USUARIO ");
            query.append("  ,PASSWORD ");
            query.append("  ,END_POINT ");
            query.append("  ,DIRECCION_FISICA ");
            query.append("FROM DIRECCION_CARGA_IMAGENES ");
            query.append("WHERE RED_ORIGEN = '").append(direccionIP).append("'");
            System.out.println(query);
            resultado = bd.executeQuery(query.toString());
            listData = sqlProperties.getColumnName(resultado);
            listData = sqlProperties.getColumnValue(resultado, listData);
            /*listData.add("PATH" + separador + "USUARIO" + separador +
                    "PASSWORD" + separador + "END_POINT" + separador + "DIRECCION_FISICA");
            while (resultado.next()) {
                String path = resultado.getString("PATH");
                path = Cifrador.desencriptarAES256(path);
                String usuario = resultado.getString("USUARIO");
                usuario = Cifrador.desencriptarAES256(usuario);
                String password = resultado.getString("PASSWORD");
                password = Cifrador.desencriptarAES256(password);
                String endPoint = resultado.getString("END_POINT");
                endPoint = Cifrador.desencriptarAES256(endPoint);
                String direccionFisica = resultado.getString("DIRECCION_FISICA");

                listData.add(path + separador + usuario + separador +
                    password + separador + endPoint + separador + direccionFisica);
            }*/
            bd.close();
        } catch (SQLException se) {
            bd.close();
            se.printStackTrace();
            throw new SQLException(msjSistema.getMensaje(131));
        }
        return listData;
    }

    public synchronized void ejecutaProcedimientoCarga(short numeroInstitucion, String filePath, int noLayout, String nombreLayout, String folioLote,
            String idUsuario, String macAddress) throws Exception {

        MensajesSistema msjSistema = new MensajesSistema();
        ArchivoError archivoError = new ArchivoError(numeroInstitucion, idUsuario, macAddress, folioLote);
        
        try {            
            filePath = filePath.replace("\\", File.separator);
            
            File fd = new File(filePath);

            listData = FileUtils.readLines(fd, "ISO-8859-1");
        } catch (IOException io) {
            io.printStackTrace();
            throw new Exception(msjSistema.getMensaje(131));
        }
       
        //listData.add("2090|NULL|ERT|15|4154|74854|78|785|78|0|01/12/2018|2.34");
        //listData.add(" |55.3|20/13/2023");
        
        int i = 0;
        for (String registro : listData) {
            boolean isOK;
            i++;
            System.out.println(i + ".- " + registro.toString());
            registro = registro.trim().replace(",", "");

            // PERSONA
            if (noLayout == 1) {
                //<editor-fold defaultstate="collapsed" desc="PERSONA">
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 5) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = persona.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 6) {
                        if (pipes < 5) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 6) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = persona.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                persona.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);
                //</editor-fold>
            }

            // CREDITO
            if (noLayout == 2) {
                //<editor-fold defaultstate="collapsed" desc="CREDITO">
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 12) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = credito.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 13) {
                        if (pipes < 12) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }

                        while (pipes < 13) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }

                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = credito.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                credito.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);
                //</editor-fold>
            }

            // GARANTIA
            if (noLayout == 3) {
                //<editor-fold defaultstate="collapsed" desc="GARANTIA">
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 14) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = garantias.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 15) {
                        if (pipes < 14) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }

                        while (pipes < 15) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }

                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = garantias.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                garantias.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);
                //</editor-fold>
            }
            
            // LINEA CREDITO
            if (noLayout == 4) {
                //<editor-fold defaultstate="collapsed" desc="LINEA CREDITO">
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 3) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = lineaCredito.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 4) {
                        if (pipes < 3) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 4) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = lineaCredito.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                lineaCredito.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);
                //</editor-fold>
            }
            
            //BIENES_ADJUDICADOS
            if (noLayout == 5) {
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 33) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = bienes.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 34) {
                        if (pipes < 33) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 34) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = bienes.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                bienes.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);

            }
            
            //OPERATIVO_PRESTAMO
            if (noLayout == 6) {
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 6) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = prestamo.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 7) {
                        if (pipes < 6) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 7) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = prestamo.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                prestamo.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);

            }
            
            //OPERATIVO_INTERBANCARIO
            if (noLayout == 7) {
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 26) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = interbancario.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 27) {
                        if (pipes < 26) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 27) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = interbancario.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                interbancario.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);

            }
            
            //SICC_CATALOGO_MINIMO
            if (noLayout == 8) {
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 3) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = catalogoMinimo.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 4) {
                        if (pipes < 3) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 4) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = catalogoMinimo.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                catalogoMinimo.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);

            }
            
            //SICC_OPERATIVOS_INVERSIONES
            if (noLayout == 9) {
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 18) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = inversiones.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 19) {
                        if (pipes < 18) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 19) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = inversiones.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                inversiones.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);

            }
            
            //SICC_CONTABLES_RECLASIFICACIONES
            if (noLayout == 10) {
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 5) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = reclasificaciones.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 6) {
                        if (pipes < 5) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 6) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = reclasificaciones.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                reclasificaciones.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);

            }
            
            //SICC_CONTABLES_UPA
            if (noLayout == 11) {
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 3) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = cotablesUPA.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 4) {
                        if (pipes < 3) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 4) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = cotablesUPA.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                cotablesUPA.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);

            }
            
            //SICC_CONTABLES_CONSOLIDACION
            if (noLayout == 12) {
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 5) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = consolidacion.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 6) {
                        if (pipes < 5) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 6) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = consolidacion.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                consolidacion.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);

            }
            
            //SICC_CONTABLES_ELIMINACIONES
            if (noLayout == 13) {
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 5) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = eliminaciones.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 6) {
                        if (pipes < 5) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 6) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = eliminaciones.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                eliminaciones.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);

            }
            
            //SICC_ESTADO_VARIACIONES
            if (noLayout == 14) {               
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 2) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = estadoVariaciones.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 3) {
                        if (pipes < 2) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 3) {
                        	registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = estadoVariaciones.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                estadoVariaciones.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);
            }
            
            //SICC_ESTADO_FLUJO_EFECTIVO
            if (noLayout == 15) {
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 2) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = flujoEfectivo.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 3) {
                        if (pipes < 2) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 3) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = flujoEfectivo.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                flujoEfectivo.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);

            }
            
            //SICC_BALANCE_GENERAL
            if (noLayout == 16) {
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 2) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = balanceGeneral.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 3) {
                        if (pipes < 2) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 3) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = balanceGeneral.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                balanceGeneral.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);

            }
            
            //SICC_ESTADO_RESULTADOS
            if (noLayout == 17) {
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 2) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = estadoResultados.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 3) {
                        if (pipes < 2) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 3) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = estadoResultados.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                estadoResultados.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);

            }
            
            // ESTADO_CUENTA
            if (noLayout == 18) {
                //<editor-fold defaultstate="collapsed" desc="PERSONA">
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 64) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = estadoCuenta.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 65) {
                        if (pipes < 64) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 65) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = estadoCuenta.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                estadoCuenta.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);
                //</editor-fold>
            }


            // ANEXO 20
            if (noLayout == 19) {
                //<editor-fold defaultstate="collapsed" desc="ANEXO20">
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 64) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = anexo20.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 65) {
                        if (pipes < 64) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 65) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = anexo20.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                
                System.out.println("Registros: "+registro);
                anexo20.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);
                //</editor-fold>
            }

            // ANEXO 21
            /*if (noLayout == 6) {
                //<editor-fold defaultstate="collapsed" desc="ANEXO21">
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 39) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = anexo21.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 40) {
                        if (pipes < 39) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 40) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = anexo21.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                anexo21.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);
                //</editor-fold>
            }*/

            // ANEXO 22
            /*if (noLayout == 7) {
                //<editor-fold defaultstate="collapsed" desc="ANEXO22">
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 88) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = anexo22.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 89) {
                        if (pipes < 88) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 89) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = anexo22.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                anexo22.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);
                //</editor-fold>
            }*/

            // AVAL
            /*if (noLayout == 9) {
                //<editor-fold defaultstate="collapsed" desc="AVAL">

                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 11) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = aval.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 12) {
                        if (pipes < 11) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 12) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = aval.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                aval.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);
                //</editor-fold>
            }*/

            

            // CODIGO CNBV
            /*if (noLayout == 13) {
                //<editor-fold defaultstate="collapsed" desc="CODIGO CNBV">
                int pipes = countOccurrences(registro.toString(), '|', 0);
                if (pipes > 2) {
                    archivoError.insertaError(null, 619, noLayout, i);
                    isOK = false;
                } else {
                    isOK = codigoCNBV.validaRegistro(registro);
                }

                if (!isOK) {
                    if (pipes < 3) {
                        if (pipes < 2) {
                            archivoError.insertaError(null, 618, noLayout, i);
                        }
                        while (pipes < 3) {
                            registro = registro + "|";
                            pipes++;
                        }
                    }
                    registro = registro.replace("||", "| |");
                    registro = registro.replace("||", "| |");
                    registro = codigoCNBV.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
                } else {
                    registro = i + "|" + registro + "|";
                }

                registro = registro.replace("||", "| |");
                registro = registro.replace("||", "| |");
                codigoCNBV.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);
                //</editor-fold>
            }*/
            
            // SICC ANEXO19
            /*if (noLayout == 14) {
            	//<editor-fold defaultstate="collapsed" desc="CODIGO CNBV">
            	int pipes = countOccurrences(registro.toString(), '|', 0);
            	if (pipes > 81) {
            		archivoError.insertaError(null, 619, noLayout, i);
            		isOK = false;
            	} else {
            		isOK = anexo19.validaRegistro(registro);
            	}
            	
            	if (!isOK) {
            		if (pipes < 82) {
            			if (pipes < 81) {
            				archivoError.insertaError(null, 618, noLayout, i);
            			}
            			while (pipes < 82) {
            				registro = registro + "|";
            				pipes++;
            			}
            		}
            		registro = registro.replace("||", "| |");
            		registro = registro.replace("||", "| |");
            		registro = anexo19.separaRegistros(registro, i, noLayout, folioLote, numeroInstitucion, idUsuario, macAddress);
            	} else {
            		registro = i + "|" + registro + "|";
            	}
            	
            	registro = registro.replace("||", "| |");
            	registro = registro.replace("||", "| |");
            	anexo19.insertaCarga(registro, numeroInstitucion, nombreLayout, folioLote, noLayout, idUsuario, macAddress);
            	//</editor-fold>
            }*/
        }

        // VALIDA SI LOS DATOS CARGADOS SON CORRECTOS
        validaDatosFaltantesSicc(numeroInstitucion, noLayout, folioLote, idUsuario, macAddress);

    }

    private void validaDatosFaltantesSicc(short numeroInstitucion, int noLayout, String folioLote, String idUsuario, String macAddress) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();

        try {
            bd = new JDBCConnectionPool();

            bd.setAutoCommit(false);
            query = new StringBuffer("VALIDA_DATOS_FALTANTES_SICC(");
            query.append(numeroInstitucion).append(", ");
            query.append(noLayout).append(", '");
            query.append(fechaProceso).append("', '");
            query.append(folioLote).append("', '");
            query.append(idUsuario).append("', '");
            query.append(macAddress).append("')");
            System.out.println(query);
            bd.executeStoreProcedure(query.toString());

            bd.commit();
            bd.close();
        } catch (SQLException e) {
            bd.rollback();
            bd.close();
            e.printStackTrace();
            throw new Exception(msjSistema.getMensaje(131));
        }
    }

    public void ejecutaProcedimientoAlta(short numeroInstitucion, int noLayout, String folioLote, String idUsuario, String macAddress) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();

        try {

            // PERSONA
            if (noLayout == 1) {
                persona.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }

            // CREDITO
            if (noLayout == 2) {
                credito.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            // SICC GARANTIAS
            if (noLayout == 3) {
                garantias.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            // LINEA CREDITO
            if (noLayout == 4) {
                lineaCredito.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            // BIENES ADJUDICADOS
            if (noLayout == 5) {
            	bienes.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            // OPERATIVO PRESTAMO 
            if (noLayout == 6) {
            	prestamo.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            // OPERATIVO INTERBANCARIO
            if (noLayout == 7) {
            	interbancario.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            // CATALOGO MINIMO
            if (noLayout == 8) {
            	catalogoMinimo.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }

            // OPERATIVOS INVERSIONES
            if (noLayout == 9) {
            	inversiones.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
         	// CONTABLES RECLASIFICACIONES
            if (noLayout == 10) {
            	reclasificaciones.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            // CONTABLES UPA
            if (noLayout == 11) {
            	cotablesUPA.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            // CONTABLES CONSOLIDACION
            if (noLayout == 12) {
            	consolidacion.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            // CONTABLES ELIMINACIONES
            if (noLayout == 13) {
            	eliminaciones.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            // ESTADO VARIACIONES
            if (noLayout == 14) {
            	estadoVariaciones.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            // ESTADO FLUJO EFECTIVO
            if (noLayout == 15) {
            	flujoEfectivo.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            // BALANCE GENERAL
            if (noLayout == 16) {
            	balanceGeneral.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            // ESTADO RESULTADOS
            if (noLayout == 17) {
            	estadoResultados.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }
            
            //ESTADO DE CUENTAS
            if (noLayout == 18) {
            	estadoCuenta.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }

            // ANEXO 20
            if (noLayout == 19) {
                anexo20.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            }

            // ANEXO 21
            //if (noLayout == 6) {
            //    anexo21.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            //}

            // ANEXO 22
            //if (noLayout == 7) {
            //    anexo22.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            //}

            // AVAL
            //if (noLayout == 9) {
            //    aval.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            //}

            // CODIGO CNBV
            //if (noLayout == 13) {
            //    codigoCNBV.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            //}
            
            // CODIGO CNBV
            //if (noLayout == 14) {
            //	anexo19.procesoAltaModificacion(numeroInstitucion, noLayout, fechaProceso, folioLote, idUsuario, macAddress);
            //}

            

            try {
                bd = new JDBCConnectionPool();
                bd.setAutoCommit(false);

                query = new StringBuffer("");
                query.append("SELECT NOMBRE_LAYOUT ");
                query.append("FROM SICC_CATALOGO_LAYOUT ");
                query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                query.append("  AND NUMERO_LAYOUT = ").append(noLayout);
                query.append("  AND STATUS = 1 ");
                System.out.println(query);
                resultado = bd.executeQuery(query.toString());

                String nombreTabla = null;
                if (resultado.next()) {
                    nombreTabla = resultado.getString("NOMBRE_LAYOUT");
                }

                query.setLength(0);
                query.append("UPDATE ").append(nombreTabla).append("_LO ");
                query.append("SET STATUS_ALTA = 'P' ");
                query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                query.append("  AND FOLIO_LOTE = '").append(folioLote).append("'");
                query.append("  AND NUMERO_CONSECUTIVO NOT IN ( ");
                query.append("    SELECT SCE.NUMERO_CONSECUTIVO ");
                query.append("    FROM SICC_CARGA_ERRORES SCE ");
                query.append("    WHERE NUMERO_INSTITUCION = SCE.NUMERO_INSTITUCION ");
                query.append("      AND NUMERO_CONSECUTIVO = SCE.NUMERO_CONSECUTIVO ");
                query.append("      AND SCE.NUMERO_FOLIO = '").append(folioLote).append("'");
                query.append("    ) ");
                query.append("  AND STATUS_CARGA = 'P' ");
                System.out.println(query);
                bd.executeUpdate(query.toString());

                bd.commit();
                bd.close();
            } catch (SQLException e) {
                bd.rollback();
                bd.close();
                e.printStackTrace();
                throw new SQLException(msjSistema.getMensaje(131));
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(msjSistema.getMensaje(131));
        }
    }

    /**
     * Obtiene el folio.
     *
     * @return
     * @throws Exception
     */
    @SuppressWarnings("CallToThreadDumpStack")
    public synchronized String obtenerFolioLote(short numeroInstitucion, int noLayout, String idUsuario, String macAddress) throws Exception {
        MensajesSistema msjSistema = new MensajesSistema();
        sqlProperties = new SQLProperties();
        Institucion institucion = new Institucion();
        SimpleDateFormat sdf = new SimpleDateFormat(sqlProperties.getFormatoSoloFechaOrion());
        String fechaActual = institucion.getFechaSistema(numeroInstitucion, Sistema.sistema.CREDITO);
        String fechaAlta = sqlProperties.getFechaInsercionFormateada(sdf.format(new Date()));

        String nomeclaturaLayout = null;

        int noConsecutivo = 0;

        Map <Integer, String> hashMap = new HashMap<>();
        hashMap.put(1, "SPER");
        hashMap.put(2, "SCRE");
        hashMap.put(3, "SGAR");
        hashMap.put(4, "SLIC");
        hashMap.put(5, "SBIA");
        hashMap.put(6, "SOPP");
        hashMap.put(7, "SOPI");
        hashMap.put(8, "SCAM");
        hashMap.put(9, "SOIN");
        hashMap.put(10, "SREC");
        hashMap.put(11, "SUPA");
        hashMap.put(12, "SCSD");
        hashMap.put(13, "SELI");
        hashMap.put(14, "SVAR");
        hashMap.put(15, "SFEF");
        hashMap.put(16, "SBAL");
        hashMap.put(17, "SERE");
        hashMap.put(18, "SECT");
        hashMap.put(19, "SA20");
        //hashMap.put(6, "SA21");
        //hashMap.put(7, "SA22");
        //hashMap.put(9, "SAVA");
        //hashMap.put(12, "SLIC");
        //hashMap.put(13, "SCCN");
        //hashMap.put(14, "SA19");

        try {
            bd = new JDBCConnectionPool();

            query = new StringBuffer();
            query.append("SELECT ISNULL(MAX(NUMERO_CONSECUTIVO) + 1, 1) ");
            query.append("FROM SICC_FOLIO_LOTE ");
            query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
            query.append(" AND NUMERO_LAYOUT = ").append(noLayout);
            query.append(" AND STATUS = 1");
            System.out.println(query);
            resultado = bd.executeQuery(query.toString());

            if (resultado.next()) {
                noConsecutivo = resultado.getInt(1);
            }
            if (noConsecutivo == 1) {
                query.setLength(0);
                query.append("INSERT INTO SICC_FOLIO_LOTE(");
                query.append("  NUMERO_INSTITUCION");
                query.append("  ,NUMERO_LAYOUT");
                query.append("  ,FECHA_PROCESO");
                query.append("  ,NUMERO_CONSECUTIVO");
                query.append("  ,STATUS");
                query.append("  ,FECHA_ALTA");
                query.append("  ,ID_USUARIO_ALTA");
                query.append("  ,MAC_ADDRESS_ALTA");
                query.append("  ,FECHA_MODIFICACION");
                query.append("  ,ID_USUARIO_MODIFICACION");
                query.append("  ,MAC_ADDRESS_MODIFICACION) ");
                query.append("VALUES(");
                query.append("  ").append(numeroInstitucion);
                query.append("  ,").append(noLayout);
                query.append("  ,'").append(fechaActual).append("'");
                query.append("  ,").append(noConsecutivo);
                query.append("  ,1");
                query.append("  ,'").append(fechaAlta).append("'");
                query.append("  ,'").append(idUsuario).append("'");
                query.append("  ,'").append(macAddress).append("'");
                query.append("  ,NULL");
                query.append("  ,NULL");
                query.append("  ,NULL)");
                System.out.println(query);
                bd.executeStatement(query.toString());

                fechaProceso = fechaActual;
                nomeclaturaLayout = generaFolioLote(fechaProceso, noLayout, noConsecutivo, hashMap);
            } else {
                resultado = null;
                query.setLength(0);
                query.append("SELECT FECHA_PROCESO ");
                query.append("FROM SICC_FOLIO_LOTE ");
                query.append("WHERE NUMERO_CONSECUTIVO = ").append(noConsecutivo - 1);
                query.append("  AND STATUS = 1 ");
                query.append("  AND NUMERO_LAYOUT = ").append(noLayout);
                System.out.println(query);
                resultado = bd.executeQuery(query.toString());

                if (resultado.next()) {
                    fechaProceso = sdf.format(resultado.getDate(1));
                }

                if (fechaProceso.equals(fechaActual)) {
                    query.setLength(0);
                    query.append("UPDATE SICC_FOLIO_LOTE ");
                    query.append("SET NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  ,NUMERO_LAYOUT = ").append(noLayout);
                    query.append("  ,FECHA_PROCESO = '").append(fechaActual).append("'");
                    query.append("  ,NUMERO_CONSECUTIVO = ").append(noConsecutivo);
                    query.append("  ,STATUS = 1");
                    query.append("  ,FECHA_ALTA = GETDATE()");
                    query.append("  ,ID_USUARIO_ALTA = '").append(idUsuario).append("'");
                    query.append("  ,MAC_ADDRESS_ALTA = '").append(macAddress).append("'");
                    query.append("  ,FECHA_MODIFICACION = NULL");
                    query.append("  ,ID_USUARIO_MODIFICACION = NULL");
                    query.append("  ,MAC_ADDRESS_MODIFICACION = NULL ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_LAYOUT = ").append(noLayout);
                    query.append("  AND STATUS = 1");
                    System.out.println(query);
                    bd.executeStatement(query.toString());

                    nomeclaturaLayout = generaFolioLote(fechaProceso, noLayout, noConsecutivo, hashMap);
                } else {
                    noConsecutivo = 1;
                    query.setLength(0);
                    query.append("UPDATE SICC_FOLIO_LOTE ");
                    query.append("SET NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  ,NUMERO_LAYOUT = ").append(noLayout);
                    query.append("  ,FECHA_PROCESO = '").append(fechaActual).append("'");
                    query.append("  ,NUMERO_CONSECUTIVO = ").append(noConsecutivo);
                    query.append("  ,STATUS = 1");
                    query.append("  ,FECHA_ALTA = GETDATE()");
                    query.append("  ,ID_USUARIO_ALTA = '").append(idUsuario).append("'");
                    query.append("  ,MAC_ADDRESS_ALTA = '").append(macAddress).append("'");
                    query.append("  ,FECHA_MODIFICACION = NULL");
                    query.append("  ,ID_USUARIO_MODIFICACION = NULL");
                    query.append("  ,MAC_ADDRESS_MODIFICACION = NULL ");
                    query.append("WHERE NUMERO_INSTITUCION = ").append(numeroInstitucion);
                    query.append("  AND NUMERO_LAYOUT = ").append(noLayout);
                    query.append("  AND STATUS = 1");
                    System.out.println(query);
                    bd.executeStatement(query.toString());

                    fechaProceso = fechaActual;
                    nomeclaturaLayout = generaFolioLote(fechaProceso, noLayout, noConsecutivo, hashMap);
                }
            }

            resultado = null;
            bd.commit();
            bd.close();
        } catch (SQLException e) {
            bd.rollback();
            bd.close();
            e.printStackTrace();
            throw new SQLException(msjSistema.getMensaje(131));
        }
        return nomeclaturaLayout;
    }

    private String generaFolioLote(String fechaProceso, int noLayout, int noConsecutivo, Map hashMap) throws Exception {
        String folioLote;
        String[] arrayDate = fechaProceso.split("/");
        String dateFotmatted = arrayDate[0] + arrayDate[1] + arrayDate[2];
        String aux = String.valueOf(noConsecutivo);

        if (aux.length() == 1) {
            aux = "00" + aux;
        } else if (aux.length() == 2) {
            aux = "0" + aux;
        }

        folioLote = dateFotmatted + "_" + aux;
        folioLote = (String) hashMap.get(noLayout) + folioLote;

        return folioLote;
    }
    public boolean verificaExisteLayout(short noInstitucion, int noLayout, String nombreArchivo) throws Exception {
		boolean existe = false;
		JDBCConnectionPool bd = null;
		try {

			bd = new JDBCConnectionPool();
			StringBuffer query = new StringBuffer();
			 query.append("SELECT NOMBRE_LAYOUT ");
             query.append("FROM SICC_CATALOGO_LAYOUT ");
             query.append("WHERE NUMERO_INSTITUCION = ").append(noInstitucion);
             query.append("  AND NUMERO_LAYOUT = ").append(noLayout);
             query.append("  AND STATUS = 1 ");
             System.out.println(query);
             resultado = bd.executeQuery(query.toString());

             String nombreTabla = null;
             if (resultado.next()) {
                 nombreTabla = resultado.getString("NOMBRE_LAYOUT");
             }
            query.setLength(0);
            query.append("SELECT 1 FROM ").append(nombreTabla).append("_LO ");
			query.append(" WHERE NUMERO_INSTITUCION = ").append(noInstitucion);
			query.append(" AND NOMBRE_ARCHIVO = '").append(nombreArchivo).append("'");
			System.out.println(query);
			ResultSet resultado = bd.executeQuery(query.toString());
			if (resultado.next()){
				existe = true;
				System.out.println(existe);
			}
			
		} catch (SQLException se) {
			bd.close();
			se.printStackTrace();
			MensajesSistema mensajeSistema = new MensajesSistema();
			throw new SQLException(mensajeSistema.getMensaje(131));
		}
		return existe;
	}
    public static void main(String[] args) throws Exception {
        SiccCargaMasivaDatosFaltantes obj = new SiccCargaMasivaDatosFaltantes("31/10/2023");
        short numeroInstitucion = 1;
        int noLayout = 19;
        //String filePath = "C:\\Users\\admin\\Documents\\Tasf\\MisAmbientes\\Clientes\\10_BCPP_SICC\\Ajustes Pruebas CICLO QA 1\\Archivos_Prueba\\SICC_CREDITO_20210621.txt";
        String filePath = "C:\\Users\\infra\\OneDrive\\Escritorio\\SICC_ANEXO20_20230202 2.txt";
//        String filePath = "C:\\WEBDAV\\SICC\\SICC_CREDITO_20160428.txt";
//        String nombreLayout = "SICC_CREDITO_20160428.txt";
//        String idUsuario = "USRCONFIG";
//        String macAddress = "00:00:00:00:00:00";
//        obj.getDatosLayout(numeroInstitucion);
//        String folioLote = obj.obtenerFolioLote(numeroInstitucion, noLayout, idUsuario, macAddress);
//
//        obj.ejecutaProcedimientoCarga(numeroInstitucion, filePath, noLayout, nombreLayout, folioLote, idUsuario, macAddress);
//        obj.ejecutaProcedimientoAlta(numeroInstitucion, noLayout, folioLote, idUsuario, macAddress);

//        String registro = "4773|||10000|||0||ID_BURO_01122017|FOLIO_BURO_122017|LEI_01122017|1|0|10000||||||";
//        System.out.println(obj.countOccurrences(registro, '|', 0));
        
//        String cadena = "999999|1000.123|MOBI_02122016|";
//        Pattern p = Pattern.compile("(\\d{0,6}+\\|)(\\d{0,4}+\\||\\d{0,4}+\\.+\\d{0,2}+\\|)([a-zA-Z0-9-_ ]{0,20}+\\|)");
//        Matcher m = p.matcher(cadena.trim());
//        boolean resultado = m.matches();
//        
//        if(resultado)
//            System.out.println("SI");
//        else
//            System.out.println("NO");
        
        obj.ejecutaProcedimientoCarga((short)1, filePath, noLayout, "SICC_ANEXO20", "SCSD31102023_008", "USRCONFIG", "00:00:00:00:00:00");
        //obj.verificaExisteLayout((short) 1, noLayout, "SICC_CONTABLES_CONSOLIDACION_20230815.txt");
        obj.ejecutaProcedimientoAlta((short)1, 19, "SCSD31102023_008", "USRCONFIG", "00:00:00:00:00:00");
        
//        String path = "C:\\glassfish3\\glassfish\\domains\\bancoppelServer\\applications";
//        ReporteCargaMasivaSicc reporte =  new ReporteCargaMasivaSicc("fe15e3ec-529d-4bef-889a-1b46ab8e1fa1", idUsuario, folioLote, path, "422", noLayout);
//        reporte.getTablaReporte();
//        reporte.getReporte();
    }
}

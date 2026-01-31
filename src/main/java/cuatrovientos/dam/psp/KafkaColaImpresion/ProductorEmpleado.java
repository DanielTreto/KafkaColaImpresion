package cuatrovientos.dam.psp.KafkaColaImpresion;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Simula la actividad de los empleados enviando trabajos de impresión.
 * Genera documentos de prueba (B/N y Color) y los envía al topic de entrada de Kafka.
 */
public class ProductorEmpleado {

    private static final String IP_PUERTO_KAFKA = "127.0.0.1:9092";
    private static final String TEMA_ENTRADA = "print-jobs-incoming";
    
    // Constantes de longitud de documentos simulados
    private static final int LONG_MEMORANDUM = 1500;
    private static final int LONG_UI = 600;
    private static final int LONG_REPORTE = 900;
    private static final int LONG_MANUAL = 1200;

    public static void main(String[] args) {
        // Configuración básica del productor
        Properties propiedades = new Properties();
        propiedades.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IP_PUERTO_KAFKA);
        propiedades.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propiedades.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> productor = new KafkaProducer<>(propiedades);

        try {
            System.out.println("===== ENVIANDO TRABAJOS DE PRUEBA =====");

            // Simulación de envío de varios trabajos desde distintos departamentos
            enviarTrabajo(productor, "Memorandum Extenso", generarInformeSimulado(LONG_MEMORANDUM), "B/N", "Miguel Goyena");
            enviarTrabajo(productor, "Diseño UI Completo", generarInformeSimulado(LONG_UI), "Color", "Ana Lopez");
            enviarTrabajo(productor, "Reporte Anual", generarInformeSimulado(LONG_REPORTE), "B/N", "Juan Perez");
            enviarTrabajo(productor, "Manual de Usuario", generarInformeSimulado(LONG_MANUAL), "Color", "Lucia Martinez");
            enviarTrabajo(productor, "Acta Reunion", "Texto corto para probar caso simple.", "B/N", "Pedro Gomez");

            System.out.println("Trabajos enviados con éxito");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            productor.close();
        }
    }

    /**
     * Genera un contenido de texto simulado de una longitud específica.
     */
    private static String generarInformeSimulado(int longitud) {
        StringBuilder sb = new StringBuilder();
        int linea = 1;
        String cabecera = "--- INFORME AUTOMATICO DE SISTEMA ---\n" +
                        "FECHA: " + java.time.LocalDate.now() + "\n" +
                        "-------------------------------------\n";
        sb.append(cabecera);
        
        while (sb.length() < longitud) {
            sb.append("Linea " + linea++ + "\n");
        }
        return sb.substring(0, Math.min(sb.length(), longitud));
    }

    /**
     * Empaqueta los datos en un objeto TrabajoImpresion y lo envía a Kafka.
     */
    private static void enviarTrabajo(KafkaProducer<String, String> productor, String titulo, String doc, String tipo, String remitente) {
        TrabajoImpresion trabajo = new TrabajoImpresion(titulo, doc, tipo, remitente);
        productor.send(new ProducerRecord<>(TEMA_ENTRADA, trabajo.toJson()));
        System.out.println("Enviado documento " + titulo + " [" + tipo + "] de " + remitente);
    }
}

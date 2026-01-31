package cuatrovientos.dam.psp.KafkaColaImpresion;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Componente que archiva los trabajos originales recibidos.
 * Escucha el topic de entrada y guarda el JSON del trabajo en una carpeta
 * organizada por el nombre del remitente.
 */
public class ArchivadorDocumentos {

    private static final String KAFKA_URL = "127.0.0.1:9092";
    private static final String TOPIC_IN = "print-jobs-incoming";
    private static final String GROUP = "archivador-group"; 
    
    // Constantes de configuración
    private static final int TIEMPO_SONDEO_MS = 200;
    private static final int RETARDO_BASE_MS = 5000;
    private static final int RETARDO_ALEATORIO_MS = 3000;

    private static final Random random = new Random();
    private static long tiempoInicio = System.currentTimeMillis();

    /**
     * Inicia el consumidor de Kafka para archivar documentos.
     */
    public static void main(String[] args) {
        // Configuración del consumidor
        Properties configuracion = new Properties();
        configuracion.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        configuracion.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configuracion.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configuracion.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        configuracion.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(configuracion);
        consumidor.subscribe(Collections.singleton(TOPIC_IN));
        
        System.out.println("===== ARCHIVADOR DE DOCUMENTOS INICIADO =====");

        try {
            while (true) {
                // Sondeo continuo de mensajes
                ConsumerRecords<String, String> registros = consumidor.poll(Duration.ofMillis(TIEMPO_SONDEO_MS));
                for (ConsumerRecord<String, String> registro : registros) {
                    try {
                        log("Procesando documento");
                        // Simulación de procesamiento de documentos
                        Thread.sleep(RETARDO_BASE_MS + random.nextInt(RETARDO_ALEATORIO_MS));
                        archivar(registro.value());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            consumidor.close();
        }
    }

    /**
     * Guarda el trabajo en el sistema de ficheros local.
     */
    private static void archivar(String jsonBruto) {
        TrabajoImpresion trabajo = TrabajoImpresion.fromJson(jsonBruto);
        
        // Limpieza del nombre del remitente para crear directorios
        String nombreSeguro = trabajo.getSender().trim().replace(" ", "_").toUpperCase();
        File dirRaiz = new File("docs_archivados/" + nombreSeguro);
        
        if (!dirRaiz.exists()) {
            dirRaiz.mkdirs();
        }

        // Generación de nombre de archivo único con timestamp
        String marcaTiempo = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String nombreFichero = "D" + marcaTiempo + ".json";
        File ficheroSalida = new File(dirRaiz, nombreFichero);
        
        try (FileWriter fw = new FileWriter(ficheroSalida)) {
            fw.write(jsonBruto);
            log("Documento archivado: " + ficheroSalida.getName());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Log con contador de tiempo de ejecución.
     */
    private static void log(String mensaje) {
        long tiempoActivo = System.currentTimeMillis() - tiempoInicio;
        System.out.println("[Archivador] [" + tiempoActivo + "ms] " + mensaje);
    }
}

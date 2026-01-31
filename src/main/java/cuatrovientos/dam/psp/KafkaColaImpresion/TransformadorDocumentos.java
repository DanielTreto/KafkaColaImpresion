package cuatrovientos.dam.psp.KafkaColaImpresion;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Componente encargado de transformar los trabajos de impresión brutos.
 * Lee del topic de entrada, divide el documento en páginas y lo envía
 * al topic correspondiente (B/N o Color).
 */
public class TransformadorDocumentos {

    private static final String SERVIDOR_KAFKA = "127.0.0.1:9092";
    private static final String TEMA_ENTRADA = "print-jobs-incoming";
    private static final String TEMA_BN = "print-docs-bn";
    private static final String TEMA_COLOR = "print-docs-color";
    private static final String ID_GRUPO = "transformador-group"; 
    
    // Constantes de configuración
    private static final int TIEMPO_SONDEO_MS = 500;
    private static final int RETARDO_BASE_MS = 6000;
    private static final int RETARDO_ALEATORIO_MS = 4000;
    private static final int TAMANO_PAGINA_CARACTERES = 400;

    private static final Random random = new Random();
    private static long tiempoInicio = System.currentTimeMillis();

    /**
     * Método principal que inicia el consumidor y el productor de Kafka.
     */
    public static void main(String[] args) {
        // Configuración del Consumidor (Entrada)
        Properties propsConsumidor = new Properties();
        propsConsumidor.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVIDOR_KAFKA);
        propsConsumidor.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsConsumidor.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsConsumidor.put(ConsumerConfig.GROUP_ID_CONFIG, ID_GRUPO);
        propsConsumidor.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Configuración del Productor (Salida transformanda)
        Properties propsProductor = new Properties();
        propsProductor.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVIDOR_KAFKA);
        propsProductor.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsProductor.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(propsConsumidor);
        KafkaProducer<String, String> productor = new KafkaProducer<>(propsProductor);

        consumidor.subscribe(Collections.singleton(TEMA_ENTRADA));
        System.out.println("===== TRANSFORMADOR DE DOCUMENTOS INICIADO =====");

        try {
            while (true) {
                // Lee nuevos trabajos en lotes
                ConsumerRecords<String, String> lote = consumidor.poll(Duration.ofMillis(TIEMPO_SONDEO_MS));
                for (ConsumerRecord<String, String> registro : lote) {
                    try {
                        log("Procesando documento");
                        // Simula tiempo de procesamiento (lectura, análisis, etc.)
                        Thread.sleep(RETARDO_BASE_MS + random.nextInt(RETARDO_ALEATORIO_MS));
                        transformar(registro.value(), productor);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            consumidor.close();
            productor.close();
        }
    }

    /**
     * Lógica de transformación: Divide el texto y enruta según tipo.
     */
    private static void transformar(String jsonBruto, KafkaProducer<String, String> productor) {
        TrabajoImpresion trabajo = TrabajoImpresion.fromJson(jsonBruto);
        
        // Determinar cola de salida
        String colaDestino = TEMA_BN;
        if ("Color".equalsIgnoreCase(trabajo.getTipo())) {
            colaDestino = TEMA_COLOR;
        }

        String textoCompleto = trabajo.getDocumento();
        int longitudTotal = textoCompleto.length();
        
        // Calcular número de páginas
        int numPaginas = (longitudTotal / TAMANO_PAGINA_CARACTERES) + ((longitudTotal % TAMANO_PAGINA_CARACTERES == 0) ? 0 : 1);
        if (numPaginas == 0) numPaginas = 1; 
        
        log("Transformando el documento: " + trabajo.getTitulo() + " -> " + numPaginas + " partes -> " + colaDestino);

        // Generar mensajes por página
        for (int i = 0; i < numPaginas; i++) {
            int inicio = i * TAMANO_PAGINA_CARACTERES;
            int fin = inicio + TAMANO_PAGINA_CARACTERES;
            if (fin > longitudTotal) fin = longitudTotal;
            
            String contenidoPagina = textoCompleto.substring(inicio, fin);

            // Formato del mensaje transformado para las impresoras
            String mensaje = "DOC: " + trabajo.getTitulo() + "\n" +
                         "PAG: " + (i + 1) + " DE " + numPaginas + "\n" +
                         "CONTENIDO:\n" + contenidoPagina;
            
            productor.send(new ProducerRecord<>(colaDestino, trabajo.getTitulo(), mensaje));
        }
    }

    /**
     * Método auxiliar para logs con marca de tiempo.
     */
    private static void log(String mensaje) {
        long tiempoActivo = System.currentTimeMillis() - tiempoInicio;
        System.out.println("[Transformador] [" + tiempoActivo + "ms] " + mensaje);
    }
}

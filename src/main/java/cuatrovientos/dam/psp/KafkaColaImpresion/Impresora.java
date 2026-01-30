package cuatrovientos.dam.psp.KafkaColaImpresion;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Impresora implements Runnable {

    private String tipo;
    private String nombre;
    private static final String SERVER = "127.0.0.1:9092";
    
    private static final int TIEMPO_SONDEO_MS = 250;
    private static final int RETARDO_BASE_MS = 10000;
    private static final int RETARDO_ALEATORIO_MS = 5000;

    private Random random = new Random();
    private long tiempoInicio;

    public Impresora(String tipo, String nombre) {
        this.tipo = tipo;
        this.nombre = nombre;
    }

    @Override
    public void run() {
        this.tiempoInicio = System.currentTimeMillis();
        String topic = "Color".equals(tipo) ? "print-docs-color" : "print-docs-bn";
        String group = "Color".equals(tipo) ? "gestor-color" : "gestor-bn";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props);
        consumidor.subscribe(Collections.singleton(topic));
        
        log("Impresora encendida");

        try {
            while (true) {
                ConsumerRecords<String, String> registros = consumidor.poll(Duration.ofMillis(TIEMPO_SONDEO_MS));
                for (ConsumerRecord<String, String> registro : registros) {
                    try {
                        String[] partes = registro.value().split("\n");
                        String info = (partes.length >= 2) ? partes[0] + " | " + partes[1] : "N/A";
                        
                        log("Imprimiendo " + info);
                        Thread.sleep(RETARDO_BASE_MS + random.nextInt(RETARDO_ALEATORIO_MS));

                        imprimir(registro.value());
                    } catch (Exception e) {
                        log("Error: " + e.getMessage());
                    }
                }
            }
        } finally { 
            consumidor.close();
        }
    }

    private void imprimir(String contenido) throws IOException {
        String dirBase = "docs_imprimidos/" + tipo + "/" + nombre; 
        File directorio = new File(dirBase);
        if (!directorio.exists()) directorio.mkdirs();

        String nombreFichero = "F_" + System.currentTimeMillis() + ".txt";
        File fichero = new File(directorio, nombreFichero);
        
        try (FileWriter fw = new FileWriter(fichero)) {
            fw.write(contenido);
            log("-> Imprimió: " + nombreFichero);
        }
    }

    private void log(String mensaje) {
        long tiempoActivo = System.currentTimeMillis() - tiempoInicio;
        System.out.println("[" + nombre + "] [" + tiempoActivo + "ms] " + mensaje);
    }
}

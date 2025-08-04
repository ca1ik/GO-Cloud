package main

import (
	"bufio"
	"fmt"
	"github.com/fsnotify/fsnotify" // Dosya sistemi değişikliklerini izlemek için
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// LogEntry struct'ı, okunacak her log satırını temsil eder
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Service   string    `json:"service"` // Hangi servisten geldiği (dosya adından tahmin edilebilir)
	Message   string    `json:"message"`
}

// CollectorConfig, toplayıcının yapılandırma ayarlarını tutar
type CollectorConfig struct {
	LogDirectory    string        // İzlenecek ana log dizini
	FilePattern     string        // İzlenecek dosya deseni (örn: "*.log")
	PollingInterval time.Duration // Yeni dosyaları kontrol etme aralığı
}

func main() {
	// Yapılandırma
	config := CollectorConfig{
		LogDirectory:    "./logs",         // Örnek log dizini, bu dizini oluşturmanız gerekecek
		FilePattern:     "*.log",          // Sadece .log uzantılı dosyaları izle
		PollingInterval: 10 * time.Second, // 10 saniyede bir yeni dosyaları kontrol et
	}

	// Log dizinini oluştur (eğer yoksa)
	if _, err := os.Stat(config.LogDirectory); os.IsNotExist(err) {
		log.Printf("Log dizini '%s' bulunamadı, oluşturuluyor...", config.LogDirectory)
		if err := os.MkdirAll(config.LogDirectory, 0755); err != nil {
			log.Fatalf("Log dizini oluşturulamadı: %v", err)
		}
	}

	fmt.Printf("GoLogInsight Log Toplayıcı başlatılıyor...\n")
	fmt.Printf("İzlenecek Dizin: %s\n", config.LogDirectory)
	fmt.Printf("Dosya Deseni: %s\n", config.FilePattern)
	fmt.Printf("Dosya Gezgini/Yol: %s\n", config.filePath)

	// Dosya izleyici başlatma
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("Dosya izleyici oluşturulamadı: %v", err)
	}
	defer watcher.Close()

	// Mevcut log dosyalarını ve açık dosya tanıtıcılarını tutmak için map
	// Her dosya için kendi okuyucusunu (scanner) ve okunan son bayt pozisyonunu tutacağız
	fileReaders := make(map[string]*bufio.Scanner)
	filePointers := make(map[string]int64) // Her dosyanın okunan son bayt pozisyonu

	var wg sync.WaitGroup // Goroutine'lerin bitmesini beklemek için

	// Yeni dosyaları ve mevcut dosyaları periyodik olarak kontrol et
	go func() {
		for {
			err := scanAndWatchFiles(watcher, config, fileReaders, filePointers, &wg)
			if err != nil {
				log.Printf("Dosya tarama hatası: %v", err)
			}
			time.Sleep(config.PollingInterval)
		}
	}()

	// Dosya sistemi olaylarını dinle
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return // Kanal kapandı
			}
			if event.Op&fsnotify.Write == fsnotify.Write {
				// Dosyaya yazma olayı tespit edildiğinde
				// Sadece zaten izlediğimiz dosyalarla ilgileniyoruz
				if _, exists := fileReaders[event.Name]; exists {
					wg.Add(1)
					go processFileChanges(event.Name, fileReaders, filePointers, &wg)
				}
			} else if event.Op&fsnotify.Create == fsnotify.Create {
				// Yeni dosya oluşturulduğunda
				fmt.Printf("Yeni dosya oluşturuldu: %s\n", event.Name)
				// Yeni dosyayı watcher'a eklemek ve okumaya başlamak için tekrar tarama tetikle
				// Bu, scanAndWatchFiles döngüsü tarafından otomatik olarak ele alınacak
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return // Kanal kapandı
			}
			log.Printf("Watcher hatası: %v", err)
		}
	}
}

// scanAndWatchFiles, dizindeki mevcut ve yeni log dosyalarını bulur ve izlemeye başlar
func scanAndWatchFiles(watcher *fsnotify.Watcher, config CollectorConfig,
	fileReaders map[string]*bufio.Scanner, filePointers map[string]int64, wg *sync.WaitGroup) error {

	files, err := filepath.Glob(filepath.Join(config.LogDirectory, config.FilePattern))
	if err != nil {
		return fmt.Errorf("dosya deseni okunamadı: %v", err)
	}

	for _, filePath := range files {
		// Eğer dosya zaten izlenmiyorsa, watcher'a ekle ve okumaya başla
		if _, ok := fileReaders[filePath]; !ok {
			fmt.Printf("İzlemeye başlandı: %s\n", filePath)
			err := watcher.Add(filePath)
			if err != nil {
				log.Printf("Watcher'a dosya eklenemedi '%s': %v", filePath, err)
				continue
			}

			file, err := os.Open(filePath)
			if err != nil {
				log.Printf("Dosya açılamadı '%s': %v", filePath, err)
				continue
			}

			// Mevcut dosyaları baştan okumak yerine sonundan başla
			// Bu, toplayıcının her yeniden başlatıldığında eski logları tekrar işlemesini engeller.
			info, err := file.Stat()
			if err != nil {
				log.Printf("Dosya bilgisi alınamadı '%s': %v", filePath, err)
				file.Close()
				continue
			}
			file.Seek(info.Size(), io.SeekStart) // Dosya sonuna git

			fileReaders[filePath] = bufio.NewScanner(file)
			filePointers[filePath] = info.Size()
		}
	}
	return nil
}

// processFileChanges, belirli bir dosyadaki yeni satırları okur ve işler
func processFileChanges(filePath string, fileReaders map[string]*bufio.Scanner,
	filePointers map[string]int64, wg *sync.WaitGroup) {
	defer wg.Done()

	scanner := fileReaders[filePath]
	file, _ := scanner.Unwrap().(*os.File) // scanner'ın altında yatan *os.File nesnesini al

	// Dosyanın mevcut boyutunu al
	info, err := file.Stat()
	if err != nil {
		log.Printf("Dosya bilgisi alınamadı '%s': %v", filePath, err)
		return
	}

	// Eğer dosya boyutu küçüldüyse (log rotation vb.), başa dön
	if info.Size() < filePointers[filePath] {
		fmt.Printf("Dosya boyutu küçüldü (muhtemelen rotasyon): %s. Baştan okunuyor.\n", filePath)
		file.Seek(0, io.SeekStart)
		filePointers[filePath] = 0
	} else {
		// Mevcut pozisyondan okumaya devam et
		file.Seek(filePointers[filePath], io.SeekStart)
	}

	for scanner.Scan() {
		line := scanner.Text()
		entry := parseLogLine(filePath, line) // Log satırını ayrıştır
		sendToProcessor(entry)                // İşleyiciye gönder (şu an konsola yazıyor)
	}

	// Okuma bittikten sonra dosyanın yeni pozisyonunu kaydet
	newPosition, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Printf("Dosya pozisyonu alınamadı '%s': %v", filePath, err)
		return
	}
	filePointers[filePath] = newPosition

	if err := scanner.Err(); err != nil {
		log.Printf("Dosya okuma hatası '%s': %v", filePath, err)
	}
}

// parseLogLine, basit bir log satırını LogEntry yapısına dönüştürür.
// Burası, gerçek log formatınıza göre özelleştirilmelidir.
func parseLogLine(filePath, line string) LogEntry {
	serviceName := strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath))
	return LogEntry{
		Timestamp: time.Now(), // Basitlik için şu anki zaman
		Service:   serviceName,
		Message:   line,
	}
}

// sendToProcessor, ayrıştırılmış log girdisini bir sonraki aşamaya (işleyiciye) gönderir.
// Gerçek bir uygulamada burası Kafka, NATS, gRPC veya HTTP çağrısı olabilir.
func sendToProcessor(entry LogEntry) {
	// Şimdilik sadece konsola yazdırıyoruz.
	// fmt.Printf("Log Gönderildi: [%s] [%s] %s\n", entry.Service, entry.Timestamp.Format(time.RFC3339), entry.Message)

	// JSON formatında yazdırma (daha gerçekçi bir çıktı için)
	fmt.Printf("Log Gönderildi: %s\n", entry.String())
}

// LogEntry için String() metodu (JSON formatında çıktı için)
func (le LogEntry) String() string {
	return fmt.Sprintf(`{"timestamp": "%s", "service": "%s", "message": "%s"}`,
		le.Timestamp.Format(time.RFC3339Nano), le.Service, le.Message)
}

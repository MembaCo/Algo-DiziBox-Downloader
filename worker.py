# @author: MembaCo.

import sqlite3
import time
import subprocess
import re
import os
import sys
import logging
import glob
import shutil
from seleniumwire import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import config
from logging_config import setup_logging
from database import get_all_settings as get_all_settings_from_db

logger = logging.getLogger(__name__)


def _update_status_worker(
    conn, item_id, item_type, status=None, source_url=None, progress=None, filepath=None
):
    table = "episodes"
    try:
        cursor = conn.cursor()
        if status:
            cursor.execute(
                f"UPDATE {table} SET status = ? WHERE id = ?", (status, item_id)
            )
        if source_url:
            cursor.execute(
                f"UPDATE {table} SET source_url = ? WHERE id = ?", (source_url, item_id)
            )
        if progress is not None:
            cursor.execute(
                f"UPDATE {table} SET progress = ? WHERE id = ?", (progress, item_id)
            )
        if filepath is not None:
            cursor.execute(
                f"UPDATE {table} SET filepath = ? WHERE id = ?", (filepath, item_id)
            )
        conn.commit()
    except sqlite3.Error as e:
        logger.error(
            f"ID {item_id} ({item_type}) için worker DB güncellemesinde hata: {e}",
            exc_info=True,
        )


def find_manifest_url(target_url, user_data_dir):
    """
    Manifest URL'sini ve isteğin başlık (header) bilgilerini bulur.
    Dinamik video linkler için gelişmiş yaklaşım.
    """
    options = uc.ChromeOptions()

    # Sadece temel seçenekler
    options.add_argument("--window-size=1280,720")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--mute-audio")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument(f"--user-agent={config.USER_AGENT}")

    driver = None
    try:
        logger.info(f"Chrome başlatılıyor, profil dizini: {user_data_dir}")
        driver = uc.Chrome(user_data_dir=user_data_dir, options=options)
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        logger.info(f"Ana sayfa yükleniyor: {target_url}")
        driver.get(target_url)

        # Sayfanın tam yüklenmesini bekle
        time.sleep(5)

        # Video iframe'i bekle - daha esnek selector
        wait = WebDriverWait(driver, 30)
        logger.info("Video iframe aranıyor...")

        # Birden fazla selector dene
        iframe_selectors = [
            "div.video-container iframe",
            "iframe[src*='stream']",
            "iframe[src*='video']",
            ".video-player iframe",
            "#video-container iframe",
            "iframe",
        ]

        iframe_element = None
        for selector in iframe_selectors:
            try:
                iframe_element = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                logger.info(f"Iframe bulundu: {selector}")
                break
            except TimeoutException:
                continue

        if not iframe_element:
            logger.error("Hiçbir video iframe bulunamadı")
            return None, None

        iframe_url = iframe_element.get_attribute("src")
        if not iframe_url:
            logger.error("Video iframe URL'si alınamadı")
            return None, None

        logger.info(f"Video iframe'e geçiliyor: {iframe_url}")

        # Referrer header'ını ayarla
        driver.execute_cdp_cmd(
            "Network.setUserAgentOverride",
            {
                "userAgent": config.USER_AGENT,
                "acceptLanguage": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
                "platform": "Win32",
            },
        )

        # Iframe'e git
        driver.get(iframe_url)

        # Sayfa yüklensin ve video player hazırlansın
        logger.info("Video player yüklenmesi bekleniyor...")
        time.sleep(10)

        # Video player'ın yüklenmesini bekle
        player_wait_script = """
            return new Promise((resolve) => {
                let attempts = 0;
                const checkPlayer = () => {
                    attempts++;
                    const videos = document.querySelectorAll('video');
                    const players = document.querySelectorAll('[id*="player"], [class*="player"], [class*="video"]');
                    
                    if (videos.length > 0 || players.length > 0 || attempts > 20) {
                        resolve(true);
                    } else {
                        setTimeout(checkPlayer, 500);
                    }
                };
                checkPlayer();
            });
        """

        try:
            driver.execute_async_script(player_wait_script)
            logger.info("Video player hazır")
        except:
            logger.info("Player bekleme timeout - devam ediliyor")

        # Overlay'leri temizle ve videoyu başlat
        start_video_script = """
            // Overlay'leri kaldır
            document.querySelectorAll('.deblocker-wrapper, .deblocker-blackout, .overlay, .ad-overlay, .modal, .popup').forEach(el => {
                try { el.style.display = 'none'; } catch(e) {}
            });
            
            // Video elementlerini bul
            const videos = document.querySelectorAll('video');
            let videoStarted = false;
            
            videos.forEach(video => {
                try {
                    video.muted = true;
                    video.autoplay = true;
                    video.play().then(() => {
                        console.log('Video started successfully');
                        videoStarted = true;
                    }).catch(e => console.log('Video play error:', e));
                } catch(e) {
                    console.log('Video manipulation error:', e);
                }
            });
            
            // Play butonlarını bul ve tıkla
            const playSelectors = [
                '.play-button', '.vjs-play-button', '.plyr__control--overlaid', 
                '.jw-display-icon-display', 'button[aria-label*="Play"]',
                'button[title*="Play"]', '.video-play-button', 
                '[class*="play"]', '[id*="play"]'
            ];
            
            playSelectors.forEach(selector => {
                document.querySelectorAll(selector).forEach(btn => {
                    try { 
                        btn.click(); 
                        console.log('Clicked play button:', selector);
                    } catch(e) {}
                });
            });
            
            return { videoElements: videos.length, videoStarted: videoStarted };
        """

        try:
            result = driver.execute_script(start_video_script)
            logger.info(f"Video başlatma sonucu: {result}")
        except Exception as js_error:
            logger.warning(f"Video başlatma script hatası: {js_error}")

        # Video yüklenmesi ve network request'lerin oluşması için bekle
        logger.info(
            "Video yüklenmesi ve manifest oluşması için 35 saniye bekleniyor..."
        )

        # Aşamalı bekleme - her 5 saniyede bir kontrol et
        for i in range(7):  # 7 x 5 = 35 saniye
            time.sleep(5)
            try:
                # Video durumunu kontrol et
                video_status = driver.execute_script("""
                    const videos = document.querySelectorAll('video');
                    if (videos.length > 0) {
                        const video = videos[0];
                        return {
                            currentTime: video.currentTime,
                            duration: video.duration,
                            readyState: video.readyState,
                            networkState: video.networkState,
                            src: video.src || video.currentSrc
                        };
                    }
                    return null;
                """)
                if video_status and video_status.get("currentTime", 0) > 0:
                    logger.info(f"Video oynatılıyor: {video_status}")
                    break
            except:
                pass

        # Network request'leri analiz et
        requests = driver.requests
        manifest_keywords = [
            ".m3u8",
            "master.txt",
            "playlist.m3u8",
            "index.m3u8",
            "manifest",
        ]
        ad_domains = [
            "video.twimg.com",
            "1king-dizibox.pages.dev",
            "googleads",
            "doubleclick",
        ]

        manifest_url = None
        headers = {}

        logger.info(f"Toplam {len(requests)} request analiz ediliyor...")

        # Manifest request'leri filtrele ve sırala
        manifest_candidates = []

        for req in requests:
            url_lower = req.url.lower()

            # Manifest dosyası kontrolü
            if any(keyword in url_lower for keyword in manifest_keywords):
                # Reklam sunucusu kontrolü
                if not any(ad_domain in url_lower for ad_domain in ad_domains):
                    # Response kontrolü
                    if hasattr(req, "response") and req.response:
                        status_code = getattr(req.response, "status_code", 0)
                        if status_code == 200:
                            # Content-Type kontrolü
                            content_type = ""
                            if hasattr(req.response, "headers"):
                                content_type = req.response.headers.get(
                                    "content-type", ""
                                ).lower()

                            manifest_candidates.append(
                                {
                                    "url": req.url,
                                    "headers": getattr(req, "headers", {}),
                                    "content_type": content_type,
                                    "timestamp": getattr(req, "date", None),
                                }
                            )

        # En uygun manifest'i seç
        if manifest_candidates:
            # Son eklenen ve en uygun olanı seç
            manifest_candidates.sort(key=lambda x: x["timestamp"] or "", reverse=True)

            for candidate in manifest_candidates:
                # .m3u8 dosyalarını öncelikle
                if ".m3u8" in candidate["url"].lower():
                    manifest_url = candidate["url"]
                    headers = {
                        "User-Agent": candidate["headers"].get(
                            "User-Agent", config.USER_AGENT
                        ),
                        "Referer": candidate["headers"].get("Referer", iframe_url),
                        "Cookie": candidate["headers"].get("Cookie", ""),
                        "Origin": "https://dizibox8.com",
                    }
                    logger.info(f"Network'ten manifest bulundu: {manifest_url[:80]}...")
                    break

        # Network'ten bulamazsak DOM'dan daha kapsamlı arama
        if not manifest_url:
            logger.info("Network'ten bulunamadı, DOM'da kapsamlı arama yapılıyor...")

            dom_sources = driver.execute_script("""
                const sources = new Set();
                
                // Video elementlerinden
                document.querySelectorAll('video').forEach(video => {
                    if (video.src) sources.add(video.src);
                    if (video.currentSrc) sources.add(video.currentSrc);
                    
                    video.querySelectorAll('source').forEach(source => {
                        if (source.src) sources.add(source.src);
                    });
                });
                
                // HLS.js veya benzeri player'lardan
                if (window.hls && window.hls.url) sources.add(window.hls.url);
                if (window.player && window.player.source) sources.add(window.player.source);
                
                // Script tag'lerinden
                document.querySelectorAll('script').forEach(script => {
                    const text = script.textContent || script.innerText || '';
                    const matches = text.match(/https?:\\/\\/[^"'\\s]+\\.(m3u8|txt|manifest)/gi);
                    if (matches) matches.forEach(match => sources.add(match));
                });
                
                // Data attribute'larından
                document.querySelectorAll('[data-src], [data-video], [data-stream]').forEach(el => {
                    ['data-src', 'data-video', 'data-stream'].forEach(attr => {
                        const val = el.getAttribute(attr);
                        if (val && val.includes('http')) sources.add(val);
                    });
                });
                
                return Array.from(sources);
            """)

            for source in dom_sources:
                if any(keyword in source.lower() for keyword in manifest_keywords):
                    # Reklam kontrolü
                    if not any(ad_domain in source.lower() for ad_domain in ad_domains):
                        manifest_url = source
                        headers = {
                            "Referer": iframe_url,
                            "User-Agent": config.USER_AGENT,
                            "Origin": "https://dizibox8.com",
                        }
                        logger.info(f"DOM'dan manifest bulundu: {manifest_url[:80]}...")
                        break

        if not manifest_url:
            logger.error("Hiçbir yöntemle manifest bulunamadı")

            # Debug bilgisi
            logger.info("Debug: Son network request'leri:")
            for req in list(requests)[-10:]:
                logger.info(f"  {req.url[:100]}")

            return None, None

        return manifest_url, headers

    except TimeoutException:
        logger.warning(f"Timeout oluştu: {target_url}")
        return None, None
    except Exception as e:
        logger.error(f"Manifest arama hatası: {e}", exc_info=True)
        return None, None
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass


def download_with_yt_dlp(
    conn, item_id, item_type, manifest_url, headers, output_template, speed_limit
):
    """
    yt-dlp ile video indirme
    """
    final_output = f"{output_template}.%(ext)s"

    cmd = [
        "yt-dlp",
        "--newline",
        "--no-check-certificates",
        "--no-color",
        "--progress",
        "--verbose",
        "--ignore-errors",
        "--hls-use-mpegts",
        "--merge-output-format",
        "mp4",
        "--format",
        "best[ext=mp4]/best",
        "--socket-timeout",
        "30",
        "--retries",
        "3",
        "--fragment-retries",
        "3",
        "-o",
        final_output,
    ]

    # Hız limiti
    if speed_limit:
        cmd.extend(["--limit-rate", speed_limit])

    # Header'lar
    if headers:
        for key, value in headers.items():
            if value:  # Boş header'ları ekleme
                cmd.extend(["--add-header", f"{key}: {value}"])

    # Referer
    if headers and headers.get("Referer"):
        cmd.extend(["--referer", headers["Referer"]])

    cmd.append(manifest_url)

    logger.info(f"yt-dlp başlatılıyor...")

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            errors="ignore",
            preexec_fn=os.setsid if sys.platform != "win32" else None,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
            if sys.platform == "win32"
            else 0,
        )

        output = ""
        last_progress = 0

        for line in iter(process.stdout.readline, ""):
            if not line:
                break

            output += line

            # Progress tracking
            progress_match = re.search(r"\[download\]\s+([0-9\.]+)%", line)
            if progress_match:
                try:
                    progress = float(progress_match.group(1))
                    if progress > last_progress:
                        _update_status_worker(
                            conn, item_id, item_type, progress=progress
                        )
                        last_progress = progress
                except:
                    pass

        process.wait()

        if process.returncode == 0:
            # İndirilen dosyayı bul
            possible_files = glob.glob(f"{output_template}.*")
            if possible_files:
                actual_file = possible_files[0]
                if (
                    os.path.exists(actual_file)
                    and os.path.getsize(actual_file) > 1024 * 1024
                ):  # 1MB+
                    return True, actual_file
                else:
                    return (
                        False,
                        f"Dosya çok küçük: {os.path.getsize(actual_file) if os.path.exists(actual_file) else 0} bytes",
                    )
            else:
                return False, "İndirme tamamlandı ama dosya bulunamadı"
        else:
            # Hata analizi
            if "403" in output or "Forbidden" in output:
                return False, "Hata: Sunucu erişimi reddetti (403)"
            elif "security error" in output.lower():
                return False, "Hata: Güvenlik hatası - DRM korumalı içerik olabilir"
            elif "404" in output:
                return False, "Hata: Video kaynağı bulunamadı (404)"
            else:
                return False, f"İndirme hatası (kod: {process.returncode})"

    except Exception as e:
        return False, f"Process hatası: {str(e)}"


def to_ascii_safe(text):
    """Dosya adları için güvenli karakter dönüşümü"""
    if not text:
        return ""

    # Türkçe karakterler
    replacements = {
        "ı": "i",
        "İ": "I",
        "ğ": "g",
        "Ğ": "G",
        "ü": "u",
        "Ü": "U",
        "ş": "s",
        "Ş": "S",
        "ö": "o",
        "Ö": "O",
        "ç": "c",
        "Ç": "C",
    }

    for tr, en in replacements.items():
        text = text.replace(tr, en)

    # Dosya sistemi için güvenli olmayan karakterleri temizle
    text = re.sub(r'[<>:"/\\|?*]', "_", text)
    text = re.sub(r"[^\x00-\x7F]+", "", text)  # ASCII olmayan

    return text.strip()


def process_video(item_id, item_type):
    """
    Ana video işleme fonksiyonu - basitleştirilmiş ve stabil
    """
    global logger
    if not logger.handlers:
        logger = setup_logging()

    conn = None
    profile_dir = None

    try:
        # Chrome profil dizini
        profile_dir = os.path.abspath(
            os.path.join("chrome_profiles", f"user_{item_id}")
        )
        if os.path.exists(profile_dir):
            shutil.rmtree(profile_dir)
        os.makedirs(profile_dir, exist_ok=True)

        # DB bağlantısı
        conn = sqlite3.connect(config.DATABASE)
        conn.row_factory = sqlite3.Row
        settings = get_all_settings_from_db(conn)

        # Episode bilgileri
        item = conn.execute(
            """
            SELECT e.*, s.season_number, ser.title as series_title 
            FROM episodes e 
            JOIN seasons s ON e.season_id = s.id 
            JOIN series ser ON s.series_id = ser.id 
            WHERE e.id = ?
        """,
            (item_id,),
        ).fetchone()

        if not item:
            logger.error(f"Episode ID {item_id} bulunamadı")
            return

        # Dosya yolu hazırla
        base_folder = settings.get("DOWNLOADS_FOLDER", "downloads")
        filename_template = settings.get(
            "SERIES_FILENAME_TEMPLATE",
            "{series_title}/Season {season_number:02d}/{series_title} - S{season_number:02d}E{episode_number:02d} - {episode_title}",
        )

        file_path = filename_template.format(
            series_title=to_ascii_safe(item["series_title"]),
            season_number=item["season_number"],
            episode_number=item["episode_number"],
            episode_title=to_ascii_safe(
                item["title"] or f"Episode_{item['episode_number']}"
            ),
        )

        full_path = os.path.join(base_folder, *file_path.split(os.path.sep))
        final_dir = os.path.dirname(full_path)
        os.makedirs(final_dir, exist_ok=True)

        output_template = os.path.join(final_dir, os.path.basename(full_path))

        logger.info(
            f"ID {item_id} başlatıldı: {item['series_title']} S{item['season_number']:02d}E{item['episode_number']:02d}"
        )

        # Kaynak arama
        _update_status_worker(conn, item_id, item_type, status="Kaynak aranıyor...")
        manifest_url, headers = find_manifest_url(item["url"], profile_dir)

        if not manifest_url:
            _update_status_worker(
                conn, item_id, item_type, status="Hata: Video kaynağı bulunamadı"
            )
            return

        # İndirme
        _update_status_worker(conn, item_id, item_type, status="İndiriliyor")
        success, result = download_with_yt_dlp(
            conn,
            item_id,
            item_type,
            manifest_url,
            headers,
            output_template,
            settings.get("SPEED_LIMIT"),
        )

        if success:
            file_path = result
            file_size = os.path.getsize(file_path)
            _update_status_worker(
                conn,
                item_id,
                item_type,
                status="Tamamlandı",
                progress=100,
                filepath=file_path,
            )
            logger.info(
                f"ID {item_id} tamamlandı: {file_path} ({file_size / 1024 / 1024:.1f}MB)"
            )
        else:
            _update_status_worker(conn, item_id, item_type, status=result)
            logger.error(f"ID {item_id} hata: {result}")

    except Exception as e:
        logger.exception(f"ID {item_id} genel hata: {e}")
        if conn:
            _update_status_worker(
                conn, item_id, item_type, status="Hata: Sistem hatası"
            )
    finally:
        if conn:
            conn.close()
        if profile_dir and os.path.exists(profile_dir):
            try:
                shutil.rmtree(profile_dir)
            except:
                pass

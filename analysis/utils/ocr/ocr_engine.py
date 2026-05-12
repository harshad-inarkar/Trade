import threading
import tesserocr
from PIL import Image, ImageOps

# ─── Thread-Local API Storage ────────────────────────────────────────────────
_thread_local = threading.local()

def get_api():
    """Fetch or initialize the Tesseract API for the current thread."""
    if not hasattr(_thread_local, "api"):
        try:
            _thread_local.api = tesserocr.PyTessBaseAPI(psm=tesserocr.PSM.SINGLE_LINE)
        except Exception as e:
            print(f"Failed to init tesserocr in thread: {e}")
            _thread_local.api = None
    return _thread_local.api


def _preprocess(img: Image.Image) -> Image.Image:
    # 1. Convert to grayscale
    img = img.convert("L")
    
    # 2. Invert (Dark mode white text -> Black text on white bg)
    img = ImageOps.invert(img)
    
    # 3. Upscale to give Tesseract more pixel density to work with
    scale = 3
    img = img.resize((img.width * scale, img.height * scale), resample=Image.Resampling.LANCZOS)
    
    # 4. Add padding (Tesseract struggles if text touches the edge)
    img = ImageOps.expand(img, border=30, fill="white")
    
    # Return the grayscale image directly. Do NOT force a manual binary threshold.
    return img

# ─── Public API ──────────────────────────────────────────────────────────────
def ocr_pil(img: Image.Image, whitelist: str = "") -> str:
    """Thread-safe, in-memory OCR function."""
    api = get_api()
    if not api: 
        return ""
    
    img = _preprocess(img)
    
    if whitelist:
        api.SetVariable("tessedit_char_whitelist", whitelist)
    else:
        api.SetVariable("tessedit_char_whitelist", "")
        
    api.SetImage(img)
    return api.GetUTF8Text().strip()

# (Optional fallback if tv_scanner ever falls back to file paths)
def ocr(image_path: str, region: tuple | None = None, whitelist: str = "") -> str:
    img = Image.open(image_path).convert("RGB")
    if region:
        x, y, w, h = region
        img = img.crop((x, y, x + w, y + h))
    return ocr_pil(img, whitelist)

# ─── CLI self-test ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python3 ocr_engine.py <image.png>")
        sys.exit(1)
    print("─── OCR result ───")
    print(ocr(sys.argv[1]))
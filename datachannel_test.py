#!/usr/bin/env python3
"""Prueba: inyectar DataChannel en una llamada de ToDus desde Termux.
Llama a 5350155246 desde la app oficial y mira el log."""
import os, re, ssl, base64, hashlib, socket, threading, time, uuid, json, asyncio, logging, fractions
import requests, urllib3
from aiortc import RTCPeerConnection, RTCSessionDescription
from aiortc.mediastreams import AudioStreamTrack, VideoStreamTrack
from av import AudioFrame, VideoFrame

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PHONE = os.environ.get("TODUS_PHONE", "5350155246")
AUTH_URL = "https://auth.todus.cu/v2/auth/token"
XMPP_HOST = os.environ.get("XMPP_HOST", "ws.todus.cu")
XMPP_PORT = int(os.environ.get("XMPP_PORT", "1756"))
XMPP_DOMAIN = "im.todus.cu"
FAKE_SECRET = "fake1234567890abcdef12345678"
RECV_BUFFER = 65536
RUNTIME = int(os.environ.get("RUNTIME", "120"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
log = logging.getLogger("dc")


def _varint(n):
    out = b""
    while True:
        b = n & 0x7F; n >>= 7
        if n: out += bytes([b | 0x80])
        else: out += bytes([b]); break
    return out


def _sf(num, val):
    if isinstance(val, str): val = val.encode()
    return bytes([(num << 3) | 2]) + _varint(len(val)) + val


def login(phone):
    phone = re.sub(r"[^\d]", "", phone)
    payload = _sf(1, phone) + _sf(2, FAKE_SECRET)
    r = requests.post(AUTH_URL, data=payload, timeout=30, verify=False,
        headers={"Content-Type": "application/x-protobuf", "User-Agent": "ToDus 2.1.2 Auth"})
    r.raise_for_status()
    m = re.search(r"eyJ[\w\-\.]+", r.content.decode("utf-8", "ignore"))
    if not m: raise RuntimeError(f"No JWT: {r.content[:200]}")
    return m.group(0)


def b64(s):
    if isinstance(s, str): s = s.encode()
    return base64.b64encode(s).decode()


def decode_b64(s):
    if not s: return ""
    try:
        pad = "=" * (4 - len(s) % 4)
        return base64.b64decode(s + pad).decode("utf-8")
    except Exception:
        return s


class SilenceTrack(AudioStreamTrack):
    kind = "audio"
    def __init__(self):
        super().__init__()
        self._timestamp = 0
        self._sample_rate = 48000
        self._samples_per_frame = 960
    async def recv(self):
        await asyncio.sleep(0.02)
        frame = AudioFrame(format="s16", layout="mono", samples=self._samples_per_frame)
        frame.pts = self._timestamp
        frame.sample_rate = self._sample_rate
        for plane in frame.planes:
            plane.update(bytes(self._samples_per_frame * 2))
        self._timestamp += self._samples_per_frame
        return frame


class BlackVideoTrack(VideoStreamTrack):
    kind = "video"
    def __init__(self):
        super().__init__()
        self._timestamp = 0
    async def recv(self):
        await asyncio.sleep(1/30)
        frame = VideoFrame(width=320, height=240, format="yuv420p")
        for plane in frame.planes:
            plane.update(bytes(plane.buffer_size))
        frame.pts = self._timestamp
        frame.time_base = fractions.Fraction(1, 90000)
        self._timestamp += 3000
        return frame


class Test:
    def __init__(self, phone):
        self.phone = re.sub(r"[^\d]", "", phone)
        self.jwt = None
        self.sock = None
        self.buf = b""
        self.send_lock = threading.Lock()
        self.running = True
        self.full_jid = f"{self.phone}@{XMPP_DOMAIN}"
        self.call_id = None
        self.peer_jid = None
        self.pc = None
        self.dc = None
        self.answer_sent = False
        self.dc_opened = False
        self._loop = None

    def send(self, data, label=""):
        with self.send_lock:
            raw = data.encode() if isinstance(data, str) else data
            log.info(f">>> [{label}] {raw[:400]!r}")
            try: self.sock.sendall(raw)
            except Exception as e: log.warning(f"send fallo: {e}")

    def recv_until(self, marker, timeout=10):
        self.sock.settimeout(timeout)
        data = b""
        try:
            while marker not in data:
                c = self.sock.recv(RECV_BUFFER)
                if not c: break
                data += c
        except socket.timeout: pass
        return data

    def conectar(self):
        log.info(f"LOGIN {self.phone}")
        self.jwt = login(self.phone)
        log.info(f"JWT OK")
        resource = hashlib.md5(self.phone.encode()).hexdigest() + "_Android"
        ctx = ssl.create_default_context()
        ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
        raw = socket.create_connection((XMPP_HOST, XMPP_PORT), timeout=30)
        self.sock = ctx.wrap_socket(raw, server_hostname=XMPP_HOST)
        self.sock.settimeout(30)
        self.send('<?xml version="1.0"?><stream:stream to="%s" xmlns="jc" xmlns:stream="x1" version="1.0">' % XMPP_DOMAIN, "STREAM-1")
        self.recv_until(b"</stream:features>")
        auth = b"\x00" + self.phone.encode() + b"\x00" + self.jwt.encode()
        self.send(f'<auth mechanism="PLAIN" xmlns="urn:ietf:params:xml:ns:xmpp-sasl">{b64(auth)}</auth>', "AUTH")
        r = self.recv_until(b"<ok", timeout=10)
        if b"<ok" not in r and b"<success" not in r:
            raise RuntimeError(f"SASL fallo: {r[:300]}")
        self.send('<?xml version="1.0"?><stream:stream to="%s" xmlns="jc" xmlns:stream="x1" version="1.0">' % XMPP_DOMAIN, "STREAM-2")
        self.recv_until(b"</stream:features>")
        bid = uuid.uuid4().hex
        self.send(f'<iq type="set" id="{bid}"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><resource>{resource}</resource></bind></iq>', "BIND")
        r = self.recv_until(b"</iq>", timeout=10)
        m = re.search(rb"<jid>([^<]+)</jid>", r)
        if m: self.full_jid = m.group(1).decode()
        self.send('<iq type="set" id="sess1"><session xmlns="urn:ietf:params:xml:ns:xmpp-session"/></iq>', "SESSION")
        self.recv_until(b"</iq>", timeout=10)
        self.send("<presence/>", "PRESENCE")
        time.sleep(0.3)
        log.info(f"CONECTADO {self.full_jid}")

    def reader(self):
        self.sock.settimeout(1)
        while self.running:
            try:
                c = self.sock.recv(RECV_BUFFER)
                if not c: break
                self.buf += c
                self._process()
            except socket.timeout: continue
            except Exception: return

    def keepalive(self):
        while self.running:
            time.sleep(25)
            try: self.send(" ", "KEEPALIVE")
            except Exception: return

    def _process(self):
        while True:
            text = self.buf.decode("utf-8", "replace")
            if not text: return
            i = text.find("<m ")
            if i == 0:
                end = text.find("</m>", i)
                if end == -1:
                    end = text.find("/>", i)
                    if end == -1: return
                    end += 2
                else: end += 4
                stanza = text[i:end]
                self.buf = self.buf[end:]
                self._on_message(stanza)
                continue
            i = text.find("<iq")
            if i == 0:
                end = text.find("</iq>", i)
                if end == -1:
                    sc = re.match(r"<iq\b[^>]*/>", text)
                    if sc:
                        stanza = sc.group(0)
                        self.buf = self.buf[len(stanza):]
                        self._on_iq(stanza)
                        continue
                    return
                stanza = text[:end + 5]
                self.buf = self.buf[end + 5:]
                self._on_iq(stanza)
                continue
            next_m = text.find("<m ")
            next_iq = text.find("<iq")
            candidates = [x for x in (next_m, next_iq) if x != -1]
            if not candidates:
                self.buf = b""; return
            self.buf = self.buf[min(candidates):]

    def _on_message(self, stanza):
        m_tcall = re.search(r"<tcall[^>]*?st=['\"]([^'\"]+)['\"][^>]*?cid=['\"]([^'\"]+)['\"]", stanza)
        if not m_tcall:
            m_tcall = re.search(r"<tcall[^>]*?cid=['\"]([^'\"]+)['\"][^>]*?st=['\"]([^'\"]+)['\"]", stanza)
            if not m_tcall: return
            cid, st_raw = m_tcall.group(1), m_tcall.group(2)
        else:
            st_raw, cid = m_tcall.group(1), m_tcall.group(2)
        st_decoded = decode_b64(st_raw)
        cmd = st_decoded.split(" ", 1)[0]
        m_from = re.search(r"\bf=['\"]([^'\"]+)['\"]", stanza)
        peer = m_from.group(1) if m_from else "?"
        log.info(f"*** TCALL cmd={cmd} cid={cid} peer={peer}")
        if cmd == "CALL":
            self.call_id = cid
            self.peer_jid = peer
            threading.Thread(target=self._aceptar, daemon=True).start()

    def _on_iq(self, stanza):
        q = re.search(r"<query[^>]*todus:call:status[^>]*status=['\"]([^'\"]+)['\"]", stanza)
        if not q:
            q = re.search(r"status=['\"]([^'\"]+)['\"][^>]*todus:call:status", stanza)
        if q:
            status = q.group(1)
            cm = re.search(r"content=['\"]([^'\"]+)['\"]", stanza)
            content_b64 = cm.group(1) if cm else ""
            content_json = decode_b64(content_b64)
            try:
                parsed = json.loads(content_json)
                state_dec = decode_b64(parsed.get("state", ""))
                cmd = state_dec.split(" ", 1)[0] if state_dec else ""
                payload = state_dec.split(" ", 1)[1] if " " in state_dec else ""
                log.info(f"*** STATUS={status} cmd={cmd} payload_len={len(payload)}")
                if cmd == "OFFER" and not self.answer_sent:
                    asyncio.run_coroutine_threadsafe(self._responder_offer(payload), self._loop)
                elif cmd == "ICE":
                    asyncio.run_coroutine_threadsafe(self._apply_ice(payload), self._loop)
            except Exception as e:
                log.warning(f"parse status: {e}")

    def _aceptar(self):
        time.sleep(0.5)
        if not self.peer_jid: return
        msg_id = uuid.uuid4().hex[:16]
        tc_id = uuid.uuid4().hex[:16]
        self.send(
            f'<m to="{self.peer_jid}" t="c" i="{msg_id}" xmlns="jc">'
            f'<k xmlns="x8"/>'
            f'<tcall xmlns="tcall:n" i="{tc_id}" mi="{msg_id}" st="{b64("PICKED")}" cid="{self.call_id}"/>'
            f'<b>PICKED</b></m>', "PICKED"
        )

    async def _responder_offer(self, offer_sdp):
        try:
            log.info("*** Creando RTCPeerConnection + DataChannel")
            self.pc = RTCPeerConnection()
            self.pc.addTrack(SilenceTrack())
            self.pc.addTrack(BlackVideoTrack())
            self.dc = self.pc.createDataChannel("tunel", ordered=True)
            log.info(f"*** DataChannel creado: label={self.dc.label} state={self.dc.readyState}")

            @self.dc.on("open")
            def on_open():
                log.info("=" * 60)
                log.info("*** DATA CHANNEL ABIERTO ***")
                log.info("=" * 60)
                self.dc_opened = True
                asyncio.create_task(self._test_send())

            @self.dc.on("message")
            def on_message(msg):
                log.info(f"*** DC recibido: {msg[:200]!r}")

            @self.dc.on("close")
            def on_close():
                log.info("*** DC cerrado")

            @self.pc.on("icecandidate")
            async def on_ice(candidate):
                if candidate:
                    await self._enviar_ice(candidate)

            @self.pc.on("connectionstatechange")
            async def on_state():
                log.info(f"*** PC state: {self.pc.connectionState}")

            offer = RTCSessionDescription(sdp=offer_sdp, type="offer")
            await self.pc.setRemoteDescription(offer)
            answer = await self.pc.createAnswer()
            await self.pc.setLocalDescription(answer)

            if "m=application" in answer.sdp:
                log.info("=" * 60)
                log.info("*** ANSWER contiene m=application (SCTP/DataChannel) ***")
                log.info("=" * 60)
            else:
                log.warning("*** ANSWER NO contiene m=application ***")

            await self._enviar_answer(answer.sdp)
        except Exception as e:
            log.exception(f"responder_offer: {e}")

    async def _test_send(self):
        try:
            for i in range(5):
                msg = f"ping_{i}"
                self.dc.send(msg)
                log.info(f"*** DC enviado: {msg}")
                await asyncio.sleep(2)
        except Exception as e:
            log.warning(f"test_send: {e}")

    async def _enviar_answer(self, sdp):
        state_b64 = b64(f"ANSWER {sdp}")
        content = json.dumps({
            "callId": self.call_id,
            "endTime": -1,
            "msgId": f"call_{uuid.uuid4().hex}",
            "startTime": -1,
            "state": state_b64,
            "userId": self.phone + "@im.todus.cu",
            "type": 32,
        })
        iq_id = uuid.uuid4().hex
        self.send(
            f'<iq type="set" id="{iq_id}" to="{self.peer_jid}">'
            f'<query xmlns="todus:call:status" '
            f'to_user="{self.peer_jid}" from_user="{self.full_jid}" '
            f'status="ANSWER" content="{b64(content)}"/>'
            f'</iq>', "ANSWER"
        )
        self.answer_sent = True
        log.info("*** ANSWER enviado")

    async def _enviar_ice(self, candidate):
        payload = f"{candidate.sdpMid} {candidate.sdpMLineIndex} {candidate.candidate}"
        state_b64 = b64(f"ICE {payload}")
        content = json.dumps({
            "callId": self.call_id,
            "endTime": -1,
            "msgId": f"call_{uuid.uuid4().hex}",
            "startTime": -1,
            "state": state_b64,
            "userId": self.phone + "@im.todus.cu",
            "type": 32,
        })
        iq_id = uuid.uuid4().hex
        self.send(
            f'<iq type="set" id="{iq_id}" to="{self.peer_jid}">'
            f'<query xmlns="todus:call:status" '
            f'to_user="{self.peer_jid}" from_user="{self.full_jid}" '
            f'status="ICE" content="{b64(content)}"/>'
            f'</iq>', "ICE"
        )

    async def _apply_ice(self, payload):
        parts = payload.split(" ", 2)
        if len(parts) != 3: return
        mid, mline, cand = parts
        try:
            from aiortc.sdp import candidate_from_sdp
            candidate = candidate_from_sdp(cand)
            candidate.sdpMid = mid
            candidate.sdpMLineIndex = int(mline)
            if self.pc:
                await self.pc.addIceCandidate(candidate)
        except Exception as e:
            log.warning(f"ICE remote: {e}")

    def close(self):
        self.running = False
        try:
            if self.dc: self.dc.close()
        except Exception: pass
        try:
            if self.pc:
                asyncio.run_coroutine_threadsafe(self.pc.close(), self._loop)
        except Exception: pass
        try:
            if self.sock:
                self.send("</stream:stream>", "CLOSE")
                self.sock.close()
        except Exception: pass


async def main_async():
    loop = asyncio.get_running_loop()
    bot = Test(PHONE)
    bot._loop = loop
    bot.conectar()
    threading.Thread(target=bot.reader, daemon=True).start()
    threading.Thread(target=bot.keepalive, daemon=True).start()
    log.info(f"LISTO - llama a {PHONE} desde la app oficial")
    log.info(f"Esperando {RUNTIME}s...")
    try:
        await asyncio.sleep(RUNTIME)
        log.info("=" * 60)
        if bot.dc_opened:
            log.info("RESULTADO: DATA CHANNEL FUNCIONA - TUNEL VIABLE")
        else:
            log.info("RESULTADO: DATA CHANNEL NO SE ABRIO - TUNEL NO VIABLE")
        log.info("=" * 60)
    finally:
        bot.close()


if __name__ == "__main__":
    asyncio.run(main_async())

<?php
namespace simplito;

use \SplFixedArray;

class LinearHashingDb {
    private $s; // split pointer
    private $n; // current number of allocated slots 
    private $m; // current hash level
    private $o; // number of stored objects

    private $base;
    private $fsize;

    private $buddySystem;
    private $pageTableDirectory;
    private $pageTableCache;
    private $pageCache;  
    private $slotCache;
    private $slotLRU;
    private $changedSlots;
    private $fd;
    private $lfd;
    private $dirtyBuddySystem;

    private $iterator;
    private $iterator_keys;

    public function __construct() {
        $this->reset();
    }

    public function __destruct() {
        $this->close();
    }

    public function open($fname, $mode) {
        $this->close();

        $rmode = $mode[0];
        $ltype = strchr($mode, "l") ? "l" : "d";
        $wait  = strchr($mode, "t") ? false : true;

        if ($rmode == "r") {
            $fmode = "rb";
            $lmode = LOCK_SH;
        } elseif ($rmode == "w") {
            $fmode = "r+b";
            $lmode = LOCK_EX;
        } elseif ($rmode == "c") {
            $fmode = "c+b";
            $lmode = LOCK_EX;
        } elseif ($rmode == "n") {
            $fmode = "w+b";
            $lmode = LOCK_EX;
        } else {
            throw new \Exception("invalid mode $mode");
        }

        if (!$wait) {
            $lmode |= LOCK_NB;
        }

        if ($ltype == 'l') {
            $lfd = fopen("$fname.lck", "c+");
            if (!$lfd) {
                return false;
            }
            if (!flock($lfd, $lmode)) {
                fclose($lfd);
                return false;
            }
            $this->lfd = $lfd; 
        }

        $fd = fopen($fname, $fmode);
        if (!$fd) {
            return false;
        }

        if ($ltype == 'd' && !flock($fd, $lmode)) {
            fclose($fd);
            return false;
        }
        $this->fd = $fd;

        set_file_buffer($this->fd, 0);

        if (fseek($this->fd, 0, SEEK_END) != 0) {
            throw new \Exception("file seek error");
        }
        $fsize = ftell($this->fd);
        if ($fsize == 0) {
            $this->initializeFile();
        } else {
            $this->readHeader();            
            $this->readPageDirectory();
            if ($rmode != "r") {
                if ($this->dirtyBuddySystem) {
                    $this->restoreBuddySystem();
                }
                else {
                    $this->readBuddySystem();
                }
            }
        }
        return true;
    }

    public function close() {
        if ($this->fd == null)
            return;

        $this->flush();
        fclose($this->fd);

        $this->fd = null;
        if ($this->lfd != null) {
            fclose($this->lfd);
            $this->lfd = null;
        }
        $this->reset();
    }

    public function insert($key, $value) {
        if ($this->fd == null) {
            throw new \Exception("invalid operation");
        }
        if ($this->exists($key)) {
            return false;
        }
        return $this->replace($key, $value);
    }

    public function replace($key, $value) {
        if ($this->fd == null) {
            throw new \Exception("invalid operation");
        }
        $slotNo = $this->slotNo($key);
        $slot   = $this->getSlot($slotNo);
        $value  = $this->prepareSlotValue($value);        

        $replace = $slot->exists($key);
        if ($replace) {
            $oldValue = $slot->fetch($key);
            if ($value == $oldValue)
                return;
            $this->freeSlotValue($oldValue);
        }

        $slot->insert($key, $value);
        $this->changedSlots[$slotNo] = $slot;

        if ($replace && ord($oldValue[0]) == 1) {
            // the space of old big value is released in buddy system 
            // so to make this replace more reliable we have to flush immediately
            $this->flush();
        }

        if (!$replace) {
            $this->o++;
            if ($this->o > $this->n) {
                $this->grow();
                $this->flush();
            }
        } 
        if (count($this->changedSlots) > 256) {
            $this->flush();
        }
    }

    public function fetch($key) {
        if ($this->fd == null) {
            throw new \Exception("invalid operation");
        }
        $slotNo = $this->slotNo($key);
        $slot   = $this->getSlot($slotNo);
        $value  = $slot->fetch($key);
        $value  = $this->getSlotValue($value);
        $this->cleanSlotCache();
        return $value;
    }

    public function exists($key) {
        if ($this->fd == null) {
            throw new \Exception("invalid operation");
        }
        $slotNo = $this->slotNo($key);
        $slot   = $this->getSlot($slotNo);
        $exists = $slot->exists($key);
        $this->cleanSlotCache();
        return $exists;        
    }

    public function delete($key) {
        if ($this->fd == null) {
            throw new \Exception("invalid operation");
        }
        $slotNo = $this->slotNo($key);
        $slot   = $this->getSlot($slotNo);
        if (!$slot->exists($key)) {
            return;
        }
        $value = $slot->fetch($key);
        $this->freeSlotValue($value);
        $slot->delete($key);
        $this->o--;        
        $this->changedSlots[$slotNo] = $slot;
        if (count($this->changedSlots) > 256) {
            $this->flush();
        }
    }

    public function firstkey() {
        if ($this->fd == null) {
            throw new \Exception("invalid operation");
        }
        $this->iterator_slot = -1;
        $this->iterator_keys = array();
        return $this->nextkey();
    }

    public function nextkey() {
        if ($this->fd == null) {
            throw new \Exception("invalid operation");
        }
        if ($this->iterator_slot >= $this->n) {
            return false;
        }
        $key = next($this->iterator_keys);
        if ($key !== false) {
            return $key;
        }

        ++$this->iterator_slot;
        if ($this->iterator_slot >= $this->n) {
            return false;
        }
        while($this->iterator_slot < $this->n) {
            $this->iterator_keys = $this->getSlot($this->iterator_slot)->getKeys();
            if (count($this->iterator_keys) > 0) {
                $this->cleanSlotCache();
                return current($this->iterator_keys);
            }
            ++$this->iterator_slot;
        }
        return false;
    }
    
    public function count() {
        if ($this->fd == null) {
            throw new \Exception("invalid operation");
        }
        return $this->o;
    }

    private function reset() {
        $this->s = 0;
        $this->n = 256;
        $this->m = 256;
        $this->o = 0;
        $this->fsize = 0;
        $this->dirtyBuddySystem = false;

        $this->buddySystem = null;
        $this->pageTableDirectory = null;
        $this->pageTableCache = null;
        $this->pageCache = array();
        $this->slotCache = array();
        $this->slotLRU = array();
        $this->changedSlots = array();
        $this->fd = null;
        $this->lfd = null;
        $this->iterator_slot = -1;
        $this->iterator_keys = array();
    }

    private function writeHeader() {
        $buf = new \PSON\ByteBuffer(256);
        $buf->writeBytes("LHDB");
        $buf->writeUint32(1);
        $buf->writeUint8($this->dirtyBuddySystem ? 1 : 0);
        $buf->writeUint32($this->base);
        $buf->writeUint32($this->fsize);
        $buf->writeUint32($this->n);
        $buf->writeUint32($this->m);
        $buf->writeUint32($this->s);
        $buf->writeUint32($this->o);
        $header = $buf->flip()->toBinary();
        $this->pwrite(0, $header);
    }

    private function readHeader() {
        $value = $this->pread(0, 256);
        $buf = \PSON\ByteBuffer::wrap($value);
        $magic = $buf->readBytes(4);
        if ($magic != "LHDB") {
            throw new \Exception("No LHBA file");
        }
        $ver   = $buf->readUint32();
        if ($ver != 1) {
            throw new \Exception("Unsupported LHBA version " . $ver);
        }
        $this->dirtyBuddySystem = $buf->readUint8() !== 0;
        $this->base = $buf->readUint32();    
        $this->fsize = $buf->readUint32();    
        $this->n = $buf->readUint32();    
        $this->m = $buf->readUint32();    
        $this->s = $buf->readUint32();    
        $this->o = $buf->readUint32();    
    }

    private function allocPageTable($pageTableNo) {
        $offset = $this->buddySystem->alloc(512*4);
        $pageTable = new SplFixedArray(512);
        for($i = 0; $i < 512; ++$i) {
            $pageTable[$i] = 0;
        }
        $this->pageTableCache[$pageTableNo] = &$pageTable;
        $this->pageTableDirectory[$pageTableNo] = $offset;
        
        $this->pwrite($offset, str_repeat(chr(0),512*4));
        $this->pwrite(256 + $pageTableNo * 4, pack("N", $offset));

        return $offset;
    }
    
    public function setBuddySystemDirty() {
        if (!$this->dirtyBuddySystem) {
            $this->dirtyBuddySystem = true;
            $this->pwrite(8, chr(1));
        }
    }
    
    private function unsetBuddySystemDirty() {
        if ($this->dirtyBuddySystem) {
            $this->dirtyBuddySystem = false;
            $this->pwrite(8, chr(0));
        }
    }

    private function allocPage($pageNo) {
        $pageTableNo  = $pageNo >> 9;
        $pageTableOff = $pageNo & 511;

        if ($this->pageTableDirectory[$pageTableNo] == 0) {
            $this->allocPageTable($pageTableNo);
        }
        
        $offset = $this->buddySystem->alloc(256*8);
        $page = new SplFixedArray(512);
        for($i = 0; $i < 512; ++$i) {
            $page[$i] = 0;
        }
        $this->pwrite($offset, str_repeat(chr(0), 256*8));

        $this->pageCache[$pageNo] = $page;

        $ptoffset = $this->pageTableDirectory[$pageTableNo] + $pageTableOff * 4;
        $this->pwrite($ptoffset, pack("N", $offset));

        $pageTable = &$this->getPageTable($pageTableNo);
        $pageTable[$pageTableOff] = $offset;

        return $offset;
    }

    private function readPageDirectory() {
        $str = $this->pread(256, 256 * 4);
        $buf = \PSON\ByteBuffer::wrap($str);
        $this->pageTableDirectory = new SplFixedArray(256);
        for($i = 0; $i < 256; ++$i) {
            $this->pageTableDirectory[$i] = $buf->readUint32();
        }
        assert($this->pageTableDirectory[0] == 256 + 256 * 4);
    }

    private function &getPageTable($pageTableNo) {
        if (isset($this->pageTableCache[$pageTableNo])) {
            return $this->pageTableCache[$pageTableNo];
        }
        $offset = $this->pageTableDirectory[$pageTableNo];
        if ($offset <= 0 || $offset >= $this->fsize) {
            throw new \Exception("invalid page table data");
        }
        $str = $this->pread($offset, 512 * 4);
        $buf = \PSON\ByteBuffer::wrap($str);
        $pageTable = new SplFixedArray(512);
        for($i = 0; $i < 512; ++$i) {
            $pageTable[$i] = $buf->readUint32();
        }
        $this->pageTableCache[$pageTableNo] = $pageTable;
        return $this->pageTableCache[$pageTableNo];
    }

    private function &getPage($pageNo) {
        if (isset($this->pageCache[$pageNo])) {
            return $this->pageCache[$pageNo];
        }
        $pageTableNo  = $pageNo >> 9;
        $pageTableOff = $pageNo & 511;
        $pageTable = &$this->getPageTable($pageTableNo);
        $offset = $pageTable[$pageTableOff];
        if ($offset <= 0 || $offset >= $this->fsize) {
            throw new \Exception("invalid page data");
        }
        $str = $this->pread($offset, 256*2*4);
        $buf = \PSON\ByteBuffer::wrap($str);
        $page = new SplFixedArray(512);
        for($i = 0; $i < 256; ++$i) {
            $page[2 * $i]     = $buf->readUint32();
            $page[2 * $i + 1] = $buf->readUint32();
        }
        $this->pageCache[$pageNo] = $page;
        return $this->pageCache[$pageNo];
    }

    private function cleanSlotCache() {
        $slotsCnt = count($this->slotCache);
        if ($slotsCnt > 1024) {
            if ($slotsCnt != count($this->slotLRU)) {
                throw new \Exception("unexpected error");
            }
            reset($this->slotLRU);
            while($slotsCnt > 256) {
                $key = key($this->slotLRU);
                if (!isset($this->changedSlots[$key])) {
                    unset($this->slotCache[$key]);
                    unset($this->slotLRU[$key]);
                    --$slotsCnt;
                    continue;
                }
                if (next($this->slotLRU) === false)
                    break;
            }
        }
    }

    private function getSlot($slotNo) {
        unset($this->slotLRU[$slotNo]);
        $this->slotLRU[$slotNo] = 1;

        if (isset($this->slotCache[$slotNo])) {
            return $this->slotCache[$slotNo];
        }

        $pageNo  = $slotNo >> 8;
        $pageOff = $slotNo & 255;
        $page = &$this->getPage($pageNo);
        $voffset = $page[2 * $pageOff];
        $vsize   = $page[2 * $pageOff + 1];
        if ($vsize == 0) {
            $slot = new Slot();
        } else {
            $value = $this->pread($voffset, $vsize);
            $slot = Slot::decode($value);
        }
        $this->slotCache[$slotNo] = $slot;
        return $slot;
    }

    private function writeUint32Array($array) {
        $buf = new \PSON\ByteBuffer(count($array) * 4);
        foreach($array as $value) {
            $buf->writeUint32($value);
        }
        $data = $buf->flip()->toBinary();
        if (($rc = fwrite($this->fd, $data)) != strlen($data)) {
            throw new \Exception("write error");
        }
    }

    private function initializeFile() {
        ftruncate($this->fd, $this->base);
        fseek($this->fd, 256);
        
        // page directory table starts at offset 256
        $this->pageTableDirectory = new SplFixedArray(256);
        for($i = 0; $i < 256; ++$i) {
            $this->pageTableDirectory[$i] = 0;
        }
        // first page directory starts at offset 1280
        $this->pageTableDirectory[0] = 256 + 256*4;
        $this->writeUint32Array($this->pageTableDirectory);

        $pageTable = new SplFixedArray(512);
        for($i = 0; $i < 512; ++$i) {
            $pageTable[$i] = 0;
        }
        // first page table starts at offset 256 +256*4 + 512*4
        $pageTable[0] = 256 + 256*4 + 512*4;
        $this->writeUint32Array($pageTable);

        $this->pageTableCache = array(0 => $pageTable);
        
        // a page contains 256  slots
        $page = new SplFixedArray(512);
        for($i = 0; $i < 512; ++$i) {
            $page[$i] = 0;
        }
        $this->writeUint32Array($page);

        $this->base  = ftell($this->fd);
        $this->fsize = $this->base;

        $this->pageCache = array(0 => $page);
        $this->buddySystem = new \simplito\BuddySystem($this->base);

        $this->writeHeader();
        $this->writeBuddySystem();
    }

    private function pread($pos, $sz) {
        if (fseek($this->fd, $pos) != 0) {
            throw new \Exception("file seek error");
        }
        $value = fread($this->fd, $sz);
        if (strlen($value) != $sz) {
            throw new \Exception("file read error");
        }
        return $value;
    }

    private function pwrite($pos, $buf) {
        $sz = strlen($buf);
        if ($pos > $this->fsize) {
            $this->setBuddySystemDirty();
            ftruncate($this->fd, $pos);
        }
        if ($pos + $sz > $this->fsize) {
            $this->setBuddySystemDirty();
            $this->fsize = $pos + $sz;
        }
        if (fseek($this->fd, $pos) != 0) {
            throw new \Exception("file fseek error");
        }
        if (($rc = fwrite($this->fd, $buf)) != $sz) {
            throw new \Exception("file write error");
        }
    }

    private function slotNo($key) {
        return Utils::slotNo($key, $this->s, $this->m);
    }

    private function prepareSlotValue($value) {
        $vsize = strlen($value);
        if ($vsize > 512) {
            $voffset = $this->buddySystem->alloc($vsize);
            $this->pwrite($voffset, $value);

            $buf = new \PSON\ByteBuffer(9);
            $buf->writeUint8(1);
            $buf->writeUint32($voffset);
            $buf->writeUint32($vsize);
            $value = $buf->flip()->toBinary();
        } else {
            $value = chr(0) . $value;
        }
        return $value;        
    }

    private function getSlotValue($value) {
        $type = ord($value[0]);
        if ($type == 0) {
            return substr($value, 1);
        } elseif ($type != 1) {
            throw new \Exception("invalid slot value");
        }
        $buf = \PSON\ByteBuffer::wrap($value);
        $buf->skip(1);
        $voffset = $buf->readUint32();
        $vsize   = $buf->readUint32();
        $value = $this->pread($voffset, $vsize);
        return $value;
    }

    private function getSlotValueInfo($value) {
        $type = ord($value[0]);
        if ($type == 0) {
            return null;
        } elseif ($type != 1) {
            throw new \Exception("invalid slot value");
        }
        $buf = \PSON\ByteBuffer::wrap($value);
        $buf->skip(1);
        $voffset = $buf->readUint32();
        $vsize   = $buf->readUint32();
        return new SlotInfo($voffset, $vsize);
    }

    private function freeSlotValue($value) {
        $type = ord($value[0]);
        if ($type == 0) {
            return;
        } elseif ($type != 1) {
            throw new \Exception("invalid slot value");
        }
        $buf = \PSON\ByteBuffer::wrap($value);
        $buf->skip(1);
        $voffset = $buf->readUint32();
        $vsize   = $buf->readUint32();
        if ($voffset > 0 && $vsize > 0) {
            $this->buddySystem->free($voffset, $vsize);
        }
    }

    public function flush() {
        if (count($this->changedSlots) == 0)
            return;
        
        $this->setBuddySystemDirty();
        foreach($this->changedSlots as $slotNo => $slot) {
            $pageNo  = $slotNo >> 8;
            $pageOff = $slotNo & 255;

            $pageTableNo = $pageNo >> 9;
            $pageTableOff = $pageNo & 511;

            $pageTable = &$this->getPageTable($pageTableNo); 
            $page = &$this->getPage($pageNo);
            
            $value = $slot->encode();
            $vsize = strlen($value);
            if ($vsize == 0) {
                $voffset = 0;
            } else {
                $voffset = $this->buddySystem->alloc($vsize);
                $this->pwrite($voffset, $value);
            }
            $this->pwrite($pageTable[$pageTableOff] + 8 * $pageOff, pack("NN", $voffset, $vsize));
            
            $ovoffset = $page[2 * $pageOff];
            $ovsize   = $page[2 * $pageOff + 1];
            if ($ovsize != 0) {
                $this->buddySystem->free($ovoffset, $ovsize);
            }
            $page[2 * $pageOff] = $voffset;
            $page[2 * $pageOff + 1] = $vsize;
        }
        $this->writeHeader();
        $this->writeBuddySystem();
        $this->unsetBuddySystemDirty();
        
        $this->changedSlots = array();
        $this->cleanSlotCache(); 
    }

    private function readBuddySystem() {
        $value = $this->pread($this->fsize, 4);
        $value = unpack("N", $value);
        $size  = $value[1];
        $value = $this->pread($this->fsize + 4, $size);
        $this->buddySystem = BuddySystem::decode($value);
    }
    
    private function restoreBuddySystem() {
        $this->buddySystem = new \simplito\BuddySystem($this->base);
        
        //Page table directory already allocated outside of buddy system
        foreach ($this->pageTableDirectory as $pageTableIndex => $pageTableOffset) {
            //End of page tables
            if ($pageTableOffset == 0) {
                break;
            }
            //First page table already allocated outside of buddy system
            if ($pageTableIndex != 0) {
                $this->buddySystem->allocExplicit($pageTableOffset, 512*4);
            }
            $pageTable = $this->getPageTable($pageTableIndex);
            foreach ($pageTable as $pageIndex => $pageOffset) {
                //End of pages
                if ($pageOffset == 0) {
                    break;
                }
                //First page in first page table already allocated outside of buddy system
                if ($pageTableIndex != 0 || $pageIndex != 0) {
                    $this->buddySystem->allocExplicit($pageOffset, 256*8);
                }
                $page = $this->getPage($pageTableIndex * 512 + $pageIndex);
                for ($slotIndex = 0; $slotIndex < 256; $slotIndex++) {
                    $slotOffset = $page[2 * $slotIndex];
                    $slotSize = $page[2 * $slotIndex + 1];
                    //Non existing slot
                    if ($slotSize == 0) {
                        continue;
                    }
                    $this->buddySystem->allocExplicit($slotOffset, $slotSize);
                    $slotData = $this->pread($slotOffset, $slotSize);
                    $slot = Slot::decode($slotData);
                    foreach ($slot->getKeys() as $key) {
                        $entryValue = $slot->fetch($key);
                        $info = $this->getSlotValueInfo($entryValue);
                        if ($info != null) {
                            $this->buddySystem->allocExplicit($info->offset, $info->size);
                        }
                    }
                }
            }
        }
        $this->writeBuddySystem();
        $this->unsetBuddySystemDirty();
    }

    private function writeBuddySystem() {
        $value = $this->buddySystem->encode();
        $size  = strlen($value);
        if (fseek($this->fd, $this->fsize) != 0) {
            throw new \Exception("file seek error");
        }
        if (fwrite($this->fd, pack("N", $size)) != 4) {
            throw new \Exception("file write error");
        }
        if (($rc = fwrite($this->fd, $value)) != $size) {
            throw new \Exception("file write error");
        }
        fflush($this->fd);    
        if (!ftruncate($this->fd, $this->fsize + 4 + $size)) {
            throw new \Exception("truncate error");
        }
    }

    private function grow() {
        if ($this->n >= 33554432) {
            return;
        }

        // allocate new page 
        $pageNo = $this->n >> 8;
        $this->allocPage($pageNo);
        $this->n += 256;

        // rehash
        for($i = $this->s; $i < $this->s + 256; ++$i) {
            $from = $this->getSlot($i);
            $to   = $this->getSlot($i + $this->m);
            $keys = $from->getKeys();
            $changed = 0;
            foreach($keys as $key) {
                $slotNo = Utils::slotNo($key, $this->s + 256, $this->m);
                if ($slotNo != $i) {
                    assert($slotNo == $i + $this->m);
                    ++$changed;
                    $value = $from->fetch($key);
                    $from->delete($key);
                    $to->insert($key, $value);
                }
            }
            if ($changed > 0) {
                $this->changedSlots[$i] = $from;
                $this->changedSlots[$i + $this->m] = $to;
            }
        }
        $this->s += 256;
        if ($this->s == $this->m) {
            $this->m = $this->m * 2;
            $this->s = 0;
        }
    }
    
    public function showBuddySystem() {
        return $this->buddySystem ? "" . $this->buddySystem : "(null)";
    }
}

class SlotInfo {
    
    public $offset;
    public $size;
    
    public function __construct($offset, $size) {
        $this->offset = $offset;
        $this->size = $size;
    }
}

class Slot {
    private $objects = array();

    public function insert($key, $value) {
        if (isset($this->objects[$key]) && $this->objects[$key] == $value)
            return;
        $this->objects[$key] = $value;
    }

    public function fetch($key) {
        if (!isset($this->objects[$key])) {
            return false;
        }
        return $this->objects[$key];
    }

    public function delete($key) {
        if (!isset($this->objects[$key]))
            return;
        unset($this->objects[$key]);
    }

    public function exists($key) {
        return isset($this->objects[$key]);
    }

    public function getKeys() {
        return array_keys($this->objects);
    }

    public function encode() {
        $sz = count($this->objects);
        if ($sz == 0) {
            return "";
        }
        $buf = new \PSON\ByteBuffer();
        $buf->writeUint16($sz);
        foreach($this->objects as $key => $value) {
            $key   = (string)$key;
            $value = (string)$value;
            $sz = strlen($key);
            $buf->writeUint32($sz);
            $buf->writeBytes($key);
            $sz = strlen($value);
            $buf->writeUint32($sz);
            $buf->writeBytes($value);
        }
        return $buf->flip()->toBinary();
    }

    public static function decode($value) {
        $objects = array();
        $buf = \PSON\ByteBuffer::wrap($value);
        if ($buf->remaining() == 0) {
            $this->objects = array();
            return;
        }
        $objs = $buf->readUint16();
        for($i = 0; $i < $objs; ++$i) {
            $sz = $buf->readUint32();
            $key = $buf->readBytes($sz);
            $sz = $buf-> readUint32();
            $value = $buf->readBytes($sz);
            $objects[$key] = $value;
        }
        $slot = new Slot();
        $slot->objects = $objects;
        return $slot;
    }
}

class Utils {
    public static function slotNo($key, $s, $m) {
        $hash = crc32($key);
        $slot = $hash & ($m - 1);
        if ($slot < $s) {
            $slot = $hash & (2 * $m - 1);
        }
        return $slot;
    }
}

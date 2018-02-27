<?php
namespace simplito;

class BuddyLevel {
    public $offsets = array();
    public $dirty = false;
    public $base;
    public $logLevel;

    public function __construct($base, $logLevel) {
        $this->base = $base;
        $this->logLevel = $logLevel;
    }

    public function count() {
        return count($this->offsets);
    }

    public function exists($offset) {
        $offset = ($offset - $this->base) >> $this->logLevel;
        return isset($this->offsets[$offset]);
    }

    public function insert($offset) {
        $offset = ($offset - $this->base) >> $this->logLevel;
        $this->offsets[$offset] = 1;
        $this->dirty = true;
    }

    public function delete($offset) {
        $offset = ($offset - $this->base) >> $this->logLevel;
        unset($this->offsets[$offset]);
    }

    public function takeFirst() {
        if ($this->dirty) {
            ksort($this->offsets);
            $this->dirty = false;
        }
        reset($this->offsets);
        $offset = key($this->offsets);
        unset($this->offsets[$offset]);
        $offset = ($offset << $this->logLevel) + $this->base;
        return $offset;
    }

    public function __toString() {
        $str = "logLevel: {$this->logLevel}, size: " . (1 <<  $this->logLevel) . ", base: " . $this->base . ", empty at (" . count($this->offsets) . "): ";
        foreach($this->offsets as $k => $v) {
            $str .= " " . ($k << $this->logLevel);
        }
        return $str;
    }

    public function getOffsets() {
        if ($this->dirty) {
            ksort($this->offsets);
            $this->dirty = false;
        }
        $offsets = array_keys($this->offsets);
        foreach($offsets as $i => $offset) {
            $offsets[$i] = ($offset << $this->logLevel) + $this->base;
        }
        return $offsets;
    }

    public function encode(\PSON\ByteBuffer $buffer) {
        $offsets = $this->getOffsets();
        $buffer->writeUint8($this->logLevel);
        $buffer->writeUint32(count($offsets));
        foreach($offsets as $offset) {
            $buffer->writeUint32($offset);
        }
        return $buffer;
    }

    public static function decode($base, \PSON\ByteBuffer $buffer) {
        $logLevel = $buffer->readUint8();
        $level = new BuddyLevel($base, $logLevel);
        $count = $buffer->readUint32();
        for($i = 0; $i < $count; ++$i) {
            $offset = $buffer->readUint32();
            $level->insert($offset);
        }
        return $level;
    }
}

class BuddySystem {
    public $levels = array();
    public $base = 0;

    function __construct($base) {
        $this->base = $base;
        for($i = 0; $i < 32; ++$i) {
            $this->levels[$i] = new BuddyLevel($base, $i);
        }
        $this->levels[31]->insert($base);
    }

    public static function logLevel($size) {
        // log10 is used intentionally, with just log the logLevel(1 << 29) returns 30
        return (int)ceil(log10($size)/log10(2));
    }

    public function buddy($offset, $size) {
        $size  = 1 << self::logLevel($size);
        $offset = $offset - $this->base;
        if ($offset & $size) {
            $offset -= $size;
        } else {
            $offset += $size;
        }
        return $offset + $this->base;
    }
    
    public function allocExplicit($offset, $size) {
        if ($offset < 0) {
            throw new \Exception("wrong offset argument $offset");
        }
        if ($size < 1) {
            throw new \Exception("wrong size argument $size");
        }
        $logLevel = self::logLevel($size);
        $level = $this->levels[$logLevel];
        while (true) {
            if ($level->exists($offset)) {
                $level->delete($offset);
                break;
            }
            $levelSize = 1 << $level->logLevel;
            $buddy = $this->buddy($offset, $levelSize);
            $level->insert($buddy);
            $offset = min($offset, $buddy);
            
            $logLevel++;
            if ($logLevel > 31) {
                throw new \Exception("cannot alloc $size bytes!");
            }
            $level = $this->levels[$logLevel];
        }
    }

    public function alloc($size) {
        if ($size < 1) {
            throw new \Exception("wrong argument $size");
        }
        $logLevel = self::logLevel($size);
        $level = $this->levels[$logLevel];
        $stack = array();
        while($level->count() == 0) {
            array_push($stack, $level);
            $logLevel++;
            if ($logLevel > 31) {
                throw new \Exception("cannot alloc $size bytes!");
            }
            $level = $this->levels[$logLevel];
        }
        $offset = $level->takeFirst();
        while(count($stack) > 0) {
            $level = array_pop($stack);
            $level->insert($offset + (1 << $level->logLevel));
        }
        return $offset;
    }

    public function free($offset, $size) {
        if ($offset < $this->base || $size < 1)
            throw new \Exception("wrong argument");

        $logLevel = self::logLevel($size);
        while($logLevel < 32) {
            $level = $this->levels[$logLevel];
            $buddy = $this->buddy($offset, $size);
            if ($level->exists($buddy)) {
                $level->delete($buddy);
                $logLevel++;
                $offset = min($offset, $buddy);
                $size = 1 << $logLevel;
            } else {
                $level->insert($offset);
                break;
            }
        }
    }
    
    public function __toStringOld() {
        $free = array();
        $str = "base: {$this->base}\nfree:";
        foreach($this->levels as $level) {
            $size = 1 << $level->logLevel;
            foreach($level->getOffsets() as $offset) {
                $free[$offset] = $offset + $size;
            }
        }
        ksort($free);
        foreach($free as $o => $s) {
            $str .= " <$o,$s>";
        }

        return $str;
    }
    
    public function __toString() {
        $free = array();
        $str = "base: {$this->base}, levels: \n";
        foreach($this->levels as $level) {
            $str .= $level . "\n";
        }
        return $str;
    }

    public function encode() {
        $buffer = new \PSON\ByteBuffer();
        $buffer->writeUint32($this->base);            
        for($i = 0; $i < 32; ++$i) {
            $this->levels[$i]->encode($buffer);
        }
        return $buffer->flip()->toBinary();
    }

    public static function decode($str) {
        $buffer = \PSON\ByteBuffer::wrap($str);
        $base = $buffer->readUint32();
        $levels = array();
        for($i = 0; $i < 32; ++$i) {
            $levels[$i] = BuddyLevel::decode($base, $buffer);
        }
        $result = new BuddySystem($base);
        $result->levels = &$levels;
        return $result;
    }
}

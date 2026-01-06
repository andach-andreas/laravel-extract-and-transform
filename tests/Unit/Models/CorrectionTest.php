<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Models;

use Andach\ExtractAndTransform\Models\Correction;
use Andach\ExtractAndTransform\Tests\TestCase;

class CorrectionTest extends TestCase
{
    public function test_it_uses_correct_table_name()
    {
        $correction = new Correction();
        $prefix = config('extract-data.internal_table_prefix', 'andach_leat_');
        $this->assertEquals($prefix . 'corrections', $correction->getTable());
    }

    public function test_it_is_guarded()
    {
        $correction = new Correction();
        // Since we use $guarded = [], we can't easily test it via isFillable without knowing attributes.
        // But we can test that we can fill attributes.
        $correction->fill(['table_name' => 'test']);
        $this->assertEquals('test', $correction->table_name);
    }
}

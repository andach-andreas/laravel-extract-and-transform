<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Services;

use Andach\ExtractAndTransform\Services\RowTransformer;
use Andach\ExtractAndTransform\Tests\TestCase;

class RowTransformerTest extends TestCase
{
    public function test_it_transforms_row_with_mapping()
    {
        $transformer = new RowTransformer;
        $row = ['first_name' => 'John', 'last_name' => 'Doe', 'age' => 30];
        $mapping = ['first_name' => 'name', 'age' => 'years'];

        $transformed = $transformer->transform($row, $mapping);

        $this->assertEquals(['name' => 'John', 'years' => 30], $transformed);
    }

    public function test_it_returns_row_as_is_without_mapping()
    {
        $transformer = new RowTransformer;
        $row = ['name' => 'John', 'age' => 30];

        $transformed = $transformer->transform($row, null);

        $this->assertEquals($row, $transformed);
    }

    public function test_it_ignores_unmapped_columns()
    {
        $transformer = new RowTransformer;
        $row = ['name' => 'John', 'age' => 30, 'extra' => 'ignored'];
        $mapping = ['name' => 'full_name'];

        $transformed = $transformer->transform($row, $mapping);

        $this->assertEquals(['full_name' => 'John'], $transformed);
    }

    public function test_it_handles_null_mapping_to_exclude_column()
    {
        $transformer = new RowTransformer;
        $row = ['name' => 'John', 'age' => 30];
        $mapping = ['name' => 'name', 'age' => null];

        $transformed = $transformer->transform($row, $mapping);

        $this->assertEquals(['name' => 'John'], $transformed);
    }
}

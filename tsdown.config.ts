import { defineConfig } from 'tsdown';
import type { UserConfig } from 'tsdown';

const config: UserConfig = defineConfig({
    entry: ['./src/index.ts'],
    outDir: './dist',
    exports: true,
    dts: true,
    format: 'esm',
    target: 'node22'
});

export default config;

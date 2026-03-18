import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        environment: 'node',
        include: ['src/**/*.test.ts'],
        coverage: {
            enabled: true,
            provider: 'v8',
            all: true,
            reportsDirectory: './coverage',
            reporter: ['text', 'lcov'],
            exclude: [
                'eslint.config.cjs',
                'tsdown.config.ts',
                'vitest.config.ts',
                'src/**/*.test.ts',
                'dist/**',
                'tmp/**',
                'node_modules/**',
                '**/*.d.ts'
            ]
        }
    }
});

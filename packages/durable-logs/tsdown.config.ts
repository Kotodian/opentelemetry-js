import { defineConfig } from 'tsdown';
import baseConfig from '../../tsdown.config.ts';

export default defineConfig({
  ...baseConfig,
  entry: [
    'src/index.ts',
    'src/exporters/http.ts',
    'src/web.ts',
    'src/desktop.ts',
    'src/testing.ts',
  ],
});

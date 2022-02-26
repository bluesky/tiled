/**
 * See https://www.npmjs.com/package/clipboard-copy
 */
declare module "clipboard-copy" {
  export default function copy(content: string): Promise<any>;
}

import axios from 'axios';

declare global {
    interface Window { baseURL: string; }
}

let baseURL = process.env.REACT_APP_API_PREFIX
if (baseURL == null) {
  let baseURL = window.baseURL;
}
let apiURL = `${baseURL}/api`

var axiosInstance = axios.create({
  baseURL: apiURL,
});

export interface IEntries {
    entries: string[];
}

export const search = async(segments: string[]): Promise<IEntries> => {
    const res = await axiosInstance.get('/node/search/' + segments.join('/'));
    console.log('search: ' + JSON.stringify(res.data));
    //var result = { hosts: res.data.hosts, success: true };
    let list: string[] = [];
    res.data.data.forEach((element: any) => {
        list.push(element.id)
    });
    console.log('items: ' + list);
    var result = { entries: list }
    return result;
}

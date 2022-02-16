import axios from 'axios';

declare global {
    interface Window { baseURL: string; }
}

let baseURL = process.env.REACT_APP_API_PREFIX
if (baseURL == null) {
  let baseURL = window.baseURL;
}
let apiURL = `${baseURL}`

var axiosInstance = axios.create({
  baseURL: apiURL,
});


export const search = async(segments: string[]): Promise<string[]> => {
    const response = await axiosInstance.get('/node/search/' + segments.join('/'));
    let ids: string[] = [];
    response.data.data.forEach((element: any) => {
        ids.push(element.id)
    });
    return ids;
}


export const metadata = async(segments: string[]): Promise<any[]> => {
    const response = await axiosInstance.get('/node/metadata/' + segments.join('/'));
    return response.data;
}
